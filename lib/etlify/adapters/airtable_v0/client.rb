require "json"
require "uri"

module Etlify
  module Adapters
    module AirtableV0
      # Low-level HTTP client for the Airtable API v0.
      # Handles request building, authentication, JSON
      # parsing, and error mapping.
      class Client
        API_BASE = "https://api.airtable.com/v0"

        attr_accessor :rate_limiter

        def initialize(access_token:, base_id:, http:)
          @access_token = access_token
          @base_id      = base_id
          @http         = http
        end

        def get(path, query: {})
          request(:get, path, query: query)
        end

        def post(path, body:)
          request(:post, path, body: body)
        end

        def patch(path, body:)
          request(:patch, path, body: body)
        end

        def delete(path, query: {})
          request(:delete, path, query: query)
        end

        def base_path(object_type)
          "/#{@base_id}/#{encode_path_segment(object_type)}"
        end

        def record_path(object_type, record_id)
          "#{base_path(object_type)}/#{encode_path_segment(record_id)}"
        end

        def raise_for_error!(response, path:)
          status = response[:status].to_i
          return if status.between?(200, 299)

          body = response[:json].is_a?(Hash) ? response[:json] : {}
          error_detail = body["error"]

          message = if error_detail.is_a?(Hash)
            error_detail["message"] || "Airtable API request failed"
          else
            body["message"] || "Airtable API request failed"
          end

          error_type = error_detail["type"] if error_detail.is_a?(Hash)
          full_message = "#{message} (status=#{status}, path=#{path}"
          full_message << ", type=#{error_type}" if error_type
          full_message << ")"

          error_class =
            case status
            when 401, 403 then Etlify::Unauthorized
            when 404      then Etlify::NotFound
            when 409, 422 then Etlify::ValidationFailed
            when 429      then Etlify::RateLimited
            else Etlify::ApiError
            end

          raise error_class.new(
            full_message,
            status: status,
            code: error_type,
            category: error_type,
            correlation_id: nil,
            details: error_detail,
            raw: response[:body]
          )
        end

        private

        def request(method, path, body: nil, query: {})
          @rate_limiter&.throttle!

          url = API_BASE + path
          unless query.empty?
            url += "?#{URI.encode_www_form(query)}"
          end

          headers = {
            "Authorization" => "Bearer #{@access_token}",
            "Content-Type" => "application/json",
            "Accept" => "application/json",
          }

          raw_body = body && JSON.dump(body)

          begin
            response = @http.request(
              method, url, headers: headers, body: raw_body
            )
          rescue => exception
            raise Etlify::TransportError.new(
              "HTTP transport error: #{exception.class}: #{exception.message}",
              status: 0,
              raw: nil
            )
          end

          response[:json] = parse_json_safe(response[:body])
          response
        end

        def encode_path_segment(segment)
          URI.encode_www_form_component(segment.to_s)
             .gsub("+", "%20")
        end

        def parse_json_safe(raw_body)
          return nil if raw_body.nil? || raw_body.empty?

          JSON.parse(raw_body)
        rescue JSON::ParserError
          nil
        end
      end
    end
  end
end
