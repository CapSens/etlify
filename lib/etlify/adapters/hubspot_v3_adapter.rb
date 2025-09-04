require "json"
require "uri"
require "net/http"

module Etlify
  module Adapters
    # HubSpot Adapter (API v3) with per-call object type.
    # It supports native objects (e.g., "contacts", "companies", "deals") and custom objects (e.g., "p12345_myobject").
    #
    # Error handling:
    # - Non-2xx responses raise specific exceptions (Unauthorized, NotFound, RateLimited, ValidationFailed, ApiError).
    # - Transport-level issues raise TransportError.
    # - delete! returns false on 404 (object not found), raises otherwise.
    #
    # Usage:
    #   adapter = Etlify::Adapters::HubspotV3Adapter.new(access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"])
    #   adapter.upsert!(object_type: "contacts", payload: { email: "john@example.com" }, id_property: "email")
    #   adapter.delete!(object_type: "contacts", crm_id: "123") # => true, or false if 404
    class HubspotV3Adapter
      API_BASE = "https://api.hubapi.com"

      # @param access_token [String] HubSpot private app token
      # @param http_client [#request] Optional HTTP client for tests. Signature: request(method, url, headers:, body:)
      def initialize(access_token:, http_client: nil)
        @access_token = access_token
        @http         = http_client || DefaultHttp.new
      end

      # Upsert by searching on id_property (if provided), otherwise create directly.
      # @param object_type [String] HubSpot CRM object type (e.g., "contacts", "companies", "deals", or a custom object)
      # @param payload [Hash] Properties for the object
      # @param id_property [String, nil] Unique property used to search and upsert
      # (e.g., "email" for contacts, "domain" for companies)
      # @return [String, nil] HubSpot hs_object_id as string or nil if not available
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        properties   = payload.dup
        unique_value = nil

        if crm_id.to_s.strip.empty?
          if id_property
            # Extract unique value whether id_property/payload keys are
            # string or symbol. Normalize, then try both forms.
            key_str = id_property.to_s
            key_sym = key_str.to_sym
            unique_value =
              properties.delete(key_str) || properties.delete(key_sym)
          end

          object_id = if id_property && unique_value
            find_object_id_by_property(object_type, id_property, unique_value)
          end
        else
          object_id = crm_id.to_s.strip
        end

        if object_id
          update_object(object_type, object_id, properties)
          object_id.to_s
        else
          create_object(object_type, properties, id_property, unique_value)
        end
      end

      # Delete an object by hs_object_id.
      # @param object_type [String]
      # @param crm_id [String]
      # @return [Boolean] true on 2xx response, false on 404
      def delete!(object_type:, crm_id:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "crm_id must be provided" if crm_id.nil? || crm_id.to_s.empty?

        path = "/crm/v3/objects/#{object_type}/#{crm_id}"
        resp = request(:delete, path)

        return true if resp[:status].between?(200, 299)
        return false if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      private

      # Simple Net::HTTP client used by default (dependency-free)
      class DefaultHttp
        def request(method, url, headers: {}, body: nil)
          uri  = URI(url)
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = uri.scheme == "https"

          klass = {
            get: Net::HTTP::Get,
            post: Net::HTTP::Post,
            patch: Net::HTTP::Patch,
            delete: Net::HTTP::Delete,
          }.fetch(method) { raise ArgumentError, "Unsupported method: #{method.inspect}" }

          req = klass.new(uri.request_uri, headers)
          req.body = body if body

          res = http.request(req)
          {status: res.code.to_i, body: res.body}
        rescue => error
          # Bubble up transport-level errors to be wrapped by #request
          raise error
        end
      end

      def request(method, path, body: nil, query: {})
        url = API_BASE + path
        url += "?#{URI.encode_www_form(query)}" unless query.empty?

        headers = {
          "Authorization" => "Bearer #{@access_token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
        }

        raw_body = body && JSON.dump(body)

        begin
          res = @http.request(method, url, headers: headers, body: raw_body)
        rescue => e
          # Normalize all transport errors into TransportError with as much context as possible
          raise Etlify::TransportError.new(
            "HTTP transport error: #{e.class}: #{e.message}",
            status: 0,
            raw: nil
          )
        end

        res[:json] = parse_json_safe(res[:body])
        res
      end

      # Centralized error raising based on status + HubSpot error shape
      def raise_for_error!(resp, path:)
        status = resp[:status].to_i
        return if status.between?(200, 299)

        payload = resp[:json].is_a?(Hash) ? resp[:json] : {}
        # HubSpot error payload commonly includes: message, category, correlationId, context, errors
        message        = payload["message"] || "HubSpot API request failed"
        category       = payload["category"]
        correlation_id = payload["correlationId"]
        details        = payload["errors"] || payload["context"]
        code           = payload["status"] || payload["errorType"] || category

        full_message = "#{message} (status=#{status}, path=#{path}"
        full_message << ", category=#{category}" if category
        full_message << ", correlationId=#{correlation_id}" if correlation_id
        full_message << ")"

        klass =
          case status
          when 401, 403 then Etlify::Unauthorized
          when 404      then Etlify::NotFound
          when 409, 422 then Etlify::ValidationFailed
          when 429      then Etlify::RateLimited
          else Etlify::ApiError
          end

        raise klass.new(
          full_message,
          status: status,
          code: code,
          category: category,
          correlation_id: correlation_id,
          details: details,
          raw: resp[:body]
        )
      end

      def parse_json_safe(str)
        return nil if str.nil? || str.empty?

        JSON.parse(str)
      rescue JSON::ParserError
        nil
      end

      def find_object_id_by_property(object_type, property, value)
        path = "/crm/v3/objects/#{object_type}/search"

        # Normalize input for safer matching on HubSpot side
        prop = property.to_s
        clean_value = value.to_s.strip
        value = (prop == "email") ? clean_value.downcase : clean_value

        # Base exact match (works for native/custom objects)
        filter_groups = [
          {
            filters: [
              {propertyName: prop, operator: "EQ", value: value},
            ],
          },
        ]

        # Contacts quirks: search secondary emails and handle "+" edge cases
        if object_type == "contacts" && prop == "email"
          # Secondary emails live in hs_additional_emails
          filter_groups << {
            filters: [
              {
                propertyName: "hs_additional_emails",
                operator: "CONTAINS_TOKEN",
                value: value,
              },
            ],
          }

          # Last-resort: try with %2B for APIs that mishandle "+"
          if value.include?("+")
            filter_groups << {
              filters: [
                {
                  propertyName: "email",
                  operator: "EQ",
                  value: value.gsub("+", "%2B"),
                },
              ],
            }
          end
        end

        body = {filterGroups: filter_groups, properties: ["hs_object_id"], limit: 1}
        resp = request(:post, path, body: body)

        if resp[:status] == 200 && resp[:json].is_a?(Hash)
          results = resp[:json]["results"]
          return results.first["id"] if results.is_a?(Array) && results.any?

          return nil
        end

        return nil if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      def update_object(object_type, object_id, properties)
        path = "/crm/v3/objects/#{object_type}/#{object_id}"
        body = {properties: stringify_keys(properties)}
        resp = request(:patch, path, body: body)
        raise_for_error!(resp, path: path)
        true
      end

      def create_object(object_type, properties, id_property, unique_value)
        path  = "/crm/v3/objects/#{object_type}"
        props = stringify_keys(properties)

        # If a unique property was provided and its value was extracted, ensure it is present on creation
        if id_property && unique_value && !props.key?(id_property.to_s)
          props[id_property.to_s] = unique_value
        end

        props["email"] = props["email"].downcase if props.key?("email")

        resp = request(:post, path, body: {properties: props})
        if resp[:status].between?(200, 299) && resp[:json].is_a?(Hash) && resp[:json]["id"]
          return resp[:json]["id"].to_s
        end

        raise_for_error!(resp, path: path)
      end

      def stringify_keys(hash)
        hash.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      end
    end
  end
end
