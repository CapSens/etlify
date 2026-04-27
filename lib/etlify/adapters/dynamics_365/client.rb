require "json"
require "uri"

module Etlify
  module Adapters
    module Dynamics365
      # Low-level HTTP client for the Microsoft Dynamics 365
      # Dataverse Web API. Handles OAuth 2.0 (client_credentials),
      # OData URL building, request execution, JSON parsing, and
      # error mapping. Wraps an injectable HTTP transport so the
      # adapter remains free of network access in tests.
      class Client
        LOGIN_BASE = "https://login.microsoftonline.com"
        DEFAULT_API_VERSION = "9.2"

        attr_accessor :rate_limiter

        def initialize(
          tenant_id:,
          client_id:,
          client_secret:,
          resource_uri:,
          api_version:,
          http:,
          token_cache:
        )
          @tenant_id     = tenant_id
          @client_id     = client_id
          @client_secret = client_secret
          @resource_uri  = resource_uri.sub(%r{/+\z}, "")
          @api_version   = api_version
          @http          = http
          @token_cache   = token_cache
        end

        # --- Public API used by the adapter ---

        def patch_by_guid(entity_set, guid, payload)
          path = guid_path(entity_set, guid)
          response = request(:patch, path, body: payload)
          raise_for_error!(response, path: path)
          response
        end

        # @return [Hash] response with :crm_id extracted from OData-EntityId
        def patch_by_alternate_key(entity_set, key_name, key_value, payload)
          path = alternate_key_path(entity_set, key_name, key_value)
          response = request(:patch, path, body: payload)
          raise_for_error!(response, path: path)
          response[:crm_id] = extract_crm_id_from_entity_id(response, path)
          response
        end

        # @return [Hash] response with :crm_id extracted from OData-EntityId
        def post_create(entity_set, payload)
          path = entity_set_path(entity_set)
          response = request(:post, path, body: payload)
          raise_for_error!(response, path: path)
          response[:crm_id] = extract_crm_id_from_entity_id(response, path)
          response
        end

        def delete_by_guid(entity_set, guid)
          path = guid_path(entity_set, guid)
          response = request(:delete, path)
          return response if response[:status] == 404

          raise_for_error!(response, path: path)
          response
        end

        def raise_for_error!(response, path:)
          status = response[:status].to_i
          return if status.between?(200, 299)

          error_detail = extract_error_detail(response)
          message = error_detail["message"] ||
            "Dynamics 365 API request failed"
          code = error_detail["code"]
          correlation_id = extract_correlation_id(response)

          full_message = "#{message} (status=#{status}, path=#{path}"
          full_message << ", code=#{code}" if code
          full_message << ")"

          raise error_class_for(status).new(
            full_message,
            status: status,
            code: code,
            category: code,
            correlation_id: correlation_id,
            details: error_detail,
            raw: response[:body]
          )
        end

        private

        # --- URL building ---

        def entity_set_path(entity_set)
          "/api/data/v#{@api_version}/#{encode_segment(entity_set)}"
        end

        def guid_path(entity_set, guid)
          "#{entity_set_path(entity_set)}(#{encode_segment(guid)})"
        end

        def alternate_key_path(entity_set, key_name, key_value)
          escaped = escape_odata_string(key_value)
          "#{entity_set_path(entity_set)}(#{key_name}='#{escaped}')"
        end

        def encode_segment(segment)
          URI.encode_www_form_component(segment.to_s).gsub("+", "%20")
        end

        def escape_odata_string(value)
          value.to_s.gsub("'", "''")
        end

        # --- HTTP execution ---

        def request(method, path, body: nil, query: {}, retried_on_401: false)
          @rate_limiter&.throttle!

          url = build_url(path, query)
          token = @token_cache.fetch { fetch_oauth_token }
          headers = base_headers(token)
          raw_body = body && JSON.dump(body)

          response = perform_http(method, url, headers, raw_body)
          response[:json] = parse_json_safe(response[:body])

          if response[:status] == 401 && !retried_on_401
            @token_cache.invalidate!
            return request(
              method,
              path,
              body: body,
              query: query,
              retried_on_401: true
            )
          end

          response
        end

        def perform_http(method, url, headers, raw_body)
          @http.request(method, url, headers: headers, body: raw_body)
        rescue => exception
          raise Etlify::TransportError.new(
            "HTTP transport error: #{exception.class}: #{exception.message}",
            status: 0,
            raw: nil
          )
        end

        def build_url(path, query)
          url = "#{@resource_uri}#{path}"
          return url if query.nil? || query.empty?

          "#{url}?#{URI.encode_www_form(query)}"
        end

        def base_headers(token)
          {
            "Authorization" => "Bearer #{token}",
            "Accept" => "application/json",
            "Content-Type" => "application/json; charset=utf-8",
            "OData-MaxVersion" => "4.0",
            "OData-Version" => "4.0",
            "If-None-Match" => "null",
          }
        end

        # --- OAuth ---

        def fetch_oauth_token
          url = "#{LOGIN_BASE}/#{@tenant_id}/oauth2/v2.0/token"
          headers = {
            "Content-Type" => "application/x-www-form-urlencoded",
            "Accept" => "application/json",
          }
          body = URI.encode_www_form(
            grant_type: "client_credentials",
            client_id: @client_id,
            client_secret: @client_secret,
            scope: "#{@resource_uri}/.default"
          )

          response = perform_http(:post, url, headers, body)
          response[:json] = parse_json_safe(response[:body])
          status = response[:status].to_i

          unless status.between?(200, 299)
            raise_oauth_error!(response, url, status)
          end

          parsed_token_entry(response)
        end

        def parsed_token_entry(response)
          data = response[:json] || {}
          access_token = data["access_token"]
          expires_in = data["expires_in"]

          unless access_token.is_a?(String) && !access_token.empty?
            raise Etlify::ApiError.new(
              "OAuth response did not include an access_token",
              status: response[:status].to_i,
              raw: response[:body]
            )
          end

          ttl = expires_in.is_a?(Numeric) ? expires_in.to_i : 0
          {token: access_token, expires_at: Time.now + ttl}
        end

        def raise_oauth_error!(response, url, status)
          data = response[:json].is_a?(Hash) ? response[:json] : {}
          error_code = data["error"]
          error_description = data["error_description"]
          full_message = [
            "OAuth token request failed",
            "(status=#{status}, url=#{url}, error=#{error_code})",
          ].join(" ")
          full_message = "#{full_message}: #{error_description}" if error_description

          klass =
            case status
            when 400, 401, 403 then Etlify::Unauthorized
            when 429 then Etlify::RateLimited
            else Etlify::ApiError
            end

          raise klass.new(
            full_message,
            status: status,
            code: error_code,
            category: error_code,
            details: data,
            raw: response[:body]
          )
        end

        # --- Response parsing ---

        def parse_json_safe(raw_body)
          return nil if raw_body.nil? || raw_body.empty?

          JSON.parse(raw_body)
        rescue JSON::ParserError
          nil
        end

        def extract_error_detail(response)
          body = response[:json].is_a?(Hash) ? response[:json] : {}
          detail = body["error"]
          detail.is_a?(Hash) ? detail : {}
        end

        def extract_correlation_id(response)
          headers = response[:headers]
          return nil unless headers.is_a?(Hash)

          values = headers["x-ms-service-request-id"] ||
            headers["X-Ms-Service-Request-Id"]
          Array(values).first
        end

        def extract_crm_id_from_entity_id(response, path)
          entity_id = entity_id_header(response)

          if entity_id.nil? || entity_id.empty?
            raise Etlify::ApiError.new(
              [
                "Dynamics response did not include OData-EntityId header",
                "(path=#{path})",
              ].join(" "),
              status: response[:status].to_i,
              raw: response[:body]
            )
          end

          match = entity_id.match(/\(([^)]+)\)\z/)
          unless match
            raise Etlify::ApiError.new(
              [
                "Could not extract crm_id from OData-EntityId header",
                "(value=#{entity_id}, path=#{path})",
              ].join(" "),
              status: response[:status].to_i,
              raw: response[:body]
            )
          end

          match[1]
        end

        def entity_id_header(response)
          headers = response[:headers]
          return nil unless headers.is_a?(Hash)

          values = headers["odata-entityid"] ||
            headers["OData-EntityId"] ||
            headers["Odata-Entityid"]
          Array(values).first
        end

        def error_class_for(status)
          case status
          when 401, 403 then Etlify::Unauthorized
          when 404      then Etlify::NotFound
          when 409, 422 then Etlify::ValidationFailed
          when 429      then Etlify::RateLimited
          else Etlify::ApiError
          end
        end
      end
    end
  end
end
