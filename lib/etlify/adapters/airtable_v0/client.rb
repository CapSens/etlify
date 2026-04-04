# frozen_string_literal: true

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
          "/#{@base_id}/#{object_type}"
        end

        def raise_for_error!(resp, path:)
          status = resp[:status].to_i
          return if status.between?(200, 299)

          payload = resp[:json].is_a?(Hash) ? resp[:json] : {}
          err = payload["error"]

          message = if err.is_a?(Hash)
            err["message"] || "Airtable API request failed"
          else
            payload["message"] || "Airtable API request failed"
          end

          type = err["type"] if err.is_a?(Hash)
          full_message = "#{message} (status=#{status}, path=#{path}"
          full_message << ", type=#{type}" if type
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
            code: type,
            category: type,
            correlation_id: nil,
            details: err,
            raw: resp[:body]
          )
        end

        private

        def request(method, path, body: nil, query: {})
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
            res = @http.request(
              method, url, headers: headers, body: raw_body
            )
          rescue => e
            raise Etlify::TransportError.new(
              "HTTP transport error: #{e.class}: #{e.message}",
              status: 0,
              raw: nil
            )
          end

          res[:json] = parse_json_safe(res[:body])
          res
        end

        def parse_json_safe(str)
          return nil if str.nil? || str.empty?

          JSON.parse(str)
        rescue JSON::ParserError
          nil
        end
      end
    end
  end
end
