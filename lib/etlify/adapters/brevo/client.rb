require "json"
require "uri"

module Etlify
  module Adapters
    module Brevo
      # Low-level HTTP client for the Brevo API v3.
      # Handles request building, authentication, JSON
      # parsing, and error mapping.
      class Client
        API_BASE = "https://api.brevo.com/v3"

        attr_accessor :rate_limiter

        def initialize(api_key:, http:)
          @api_key = api_key
          @http    = http
        end

        def get(path, query: {})
          request(:get, path, query: query)
        end

        def post(path, body:)
          request(:post, path, body: body)
        end

        def put(path, body:)
          request(:put, path, body: body)
        end

        def patch(path, body:)
          request(:patch, path, body: body)
        end

        def delete(path)
          request(:delete, path)
        end

        def raise_for_error!(response, path:)
          status = response[:status].to_i
          return if status.between?(200, 299)

          body = response[:json].is_a?(Hash) ? response[:json] : {}
          message = body["message"] || "Brevo API request failed"
          code = body["code"] || body["error"]

          full_message = "#{message} (status=#{status}, path=#{path}"
          full_message << ", code=#{code}" if code
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
            code: code,
            category: nil,
            correlation_id: nil,
            details: body,
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
            "api-key" => @api_key,
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
