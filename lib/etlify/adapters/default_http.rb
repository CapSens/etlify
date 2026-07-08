require "net/http"
require "uri"
require "openssl"

module Etlify
  module Adapters
    # Simple Net::HTTP client used by default (dependency-free).
    # Shared across adapters (Airtable, HubSpot, etc.).
    # Signature: request(method, url, headers:, body:) → {status:, body:, headers:}
    class DefaultHttp
      OPEN_TIMEOUT = 5
      READ_TIMEOUT = 30

      def request(method, url, headers: {}, body: nil)
        uri  = URI(url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = uri.scheme == "https"
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
        http.open_timeout = OPEN_TIMEOUT
        http.read_timeout = READ_TIMEOUT

        request_class = {
          get: Net::HTTP::Get,
          post: Net::HTTP::Post,
          patch: Net::HTTP::Patch,
          delete: Net::HTTP::Delete,
        }.fetch(method) { raise ArgumentError, "Unsupported method: #{method.inspect}" }

        http_request = request_class.new(uri.request_uri, headers)
        http_request.body = body if body

        response = http.request(http_request)
        {
          status: response.code.to_i,
          body: response.body,
          headers: response.to_hash,
        }
      end
    end
  end
end
