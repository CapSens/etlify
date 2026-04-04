# frozen_string_literal: true

require "net/http"
require "uri"

module Etlify
  module Adapters
    # Simple Net::HTTP client used by default (dependency-free).
    # Shared across adapters (Airtable, HubSpot, etc.).
    # Signature: request(method, url, headers:, body:) → {status:, body:}
    class DefaultHttp
      def request(method, url, headers: {}, body: nil)
        uri  = URI(url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = uri.scheme == "https"

        request_class = {
          get: Net::HTTP::Get,
          post: Net::HTTP::Post,
          patch: Net::HTTP::Patch,
          delete: Net::HTTP::Delete,
        }.fetch(method) { raise ArgumentError, "Unsupported method: #{method.inspect}" }

        http_request = request_class.new(uri.request_uri, headers)
        http_request.body = body if body

        response = http.request(http_request)
        {status: response.code.to_i, body: response.body}
      end
    end
  end
end
