require "json"
require "uri"
require "openssl"

module Etlify
  module Adapters
    module ZohoCrm
      # OAuth access token obtained from a long-lived refresh token.
      #
      # Zoho access tokens live one hour, and a refresh token only keeps a
      # limited number of access tokens alive at once (the oldest is revoked
      # beyond that). Each process refreshing on its own would revoke the
      # tokens of its siblings, so the token is shared through a cache store
      # and memoized in memory to avoid a cache read per HTTP call.
      class AccessToken
        TOKEN_PATH = "/oauth/v2/token"
        # Refresh ahead of the real expiry so a token never dies in flight.
        EXPIRY_MARGIN = 300
        # The cached expiry is shared, so without a per-process offset every
        # process would judge the token stale at the same second and refresh
        # concurrently, burning the refresh quota. With it, the earliest
        # process refreshes and the others find the new token in the cache.
        MAX_REFRESH_OFFSET = 120
        # Zoho always announces one hour. Used only when expires_in is missing
        # or not positive, so such a response cannot yield a token stale on
        # arrival (refreshed on every call) and a negative cache TTL.
        DEFAULT_EXPIRES_IN = 3600
        REFRESH_QUOTA_EXCEEDED_ERROR = "Access Denied"
        # The refresh quota is counted over 10 minutes: retrying after
        # BatchSyncJob's default 10 s would re-enqueue dozens of times.
        REFRESH_RETRY_AFTER = 60

        # @param accounts_url [String] Zoho Accounts server of the data center
        # @param client_id [String]
        # @param client_secret [String]
        # @param refresh_token [String]
        # @param http [#request] Signature: request(method, url, headers:, body:)
        # @param cache [ActiveSupport::Cache::Store, nil] store shared across
        #   processes; nil keeps the token in this process only
        def initialize(accounts_url:, client_id:, client_secret:, refresh_token:, http:, cache:)
          @accounts_url   = accounts_url
          @client_id      = client_id
          @client_secret  = client_secret
          @refresh_token  = refresh_token
          @http           = http
          @cache          = cache
          @cache_key      = "etlify:zoho_crm:access_token:#{OpenSSL::Digest::SHA256.hexdigest(refresh_token)}"
          @mutex          = Mutex.new
          @token          = nil
          @expires_at     = 0
          @refresh_offset = Random.rand(0..MAX_REFRESH_OFFSET)
        end

        # @return [String] a token valid for at least EXPIRY_MARGIN seconds
        def value
          @mutex.synchronize do
            load_from_cache! unless fresh?
            refresh! unless fresh?

            @token
          end
        end

        # Forget a token the API rejected. Only the given token is dropped, so
        # a fresher one written meanwhile by another process survives.
        def invalidate!(rejected_token)
          @mutex.synchronize do
            if @token == rejected_token
              @token = nil
              @expires_at = 0
            end

            cached = @cache&.read(@cache_key)
            @cache.delete(@cache_key) if cached.is_a?(Hash) && cached["token"] == rejected_token
          end
        end

        # The default inspect would print the client secret, the refresh token
        # and the current token, e.g. in a console or an error report.
        def inspect
          "#<#{self.class.name} accounts_url=#{@accounts_url.inspect} client_id=#{@client_id.inspect}>"
        end

        private

        def fresh?
          !@token.nil? && Time.now.to_f < @expires_at - @refresh_offset
        end

        def load_from_cache!
          cached = @cache&.read(@cache_key)
          return unless cached.is_a?(Hash)

          @token = cached["token"]
          @expires_at = cached["expires_at"].to_f
        end

        def refresh!
          url = @accounts_url + TOKEN_PATH
          body = URI.encode_www_form(
            refresh_token: @refresh_token,
            client_id: @client_id,
            client_secret: @client_secret,
            grant_type: "refresh_token"
          )

          begin
            response = @http.request(
              :post,
              url,
              headers: {"Content-Type" => "application/x-www-form-urlencoded", "Accept" => "application/json"},
              body: body
            )
          rescue => error_detail
            raise Etlify::TransportError.new(
              "HTTP transport error: #{error_detail.class}: #{error_detail.message}",
              status: 0,
              raw: nil
            )
          end

          json = parse_json_safe(response[:body])
          token = json.is_a?(Hash) ? json["access_token"] : nil
          raise_refresh_error!(response, json) if token.to_s.empty?

          expires_in = json["expires_in"].to_i
          expires_in = DEFAULT_EXPIRES_IN unless expires_in.positive?
          # A lifetime shorter than the margin would leave the token stale on
          # arrival, so the margin never eats more than half of it.
          lifetime = expires_in - [EXPIRY_MARGIN, expires_in / 2].min
          @token = token
          @expires_at = Time.now.to_f + lifetime
          @cache&.write(
            @cache_key,
            {"token" => @token, "expires_at" => @expires_at},
            expires_in: lifetime
          )
        end

        # Zoho answers a rejected refresh with a 200 and {"error": "..."}.
        # Any failure short of 429 / 5xx is reported as Unauthorized: it says
        # nothing about the record being synced, so it must never be taken
        # for a per-record error that the batch synchronizer would isolate.
        # "Access Denied" is Zoho's quota of 10 refreshes per 10 minutes per
        # refresh token: transient, so it is retried later like a 429.
        def raise_refresh_error!(response, json)
          status = response[:status].to_i
          code = json.is_a?(Hash) ? json["error"] : nil
          message = "Zoho access token refresh failed (status=#{status}, path=#{TOKEN_PATH}"
          message << ", error=#{code}" if code
          message << ")"

          options = {status: status, code: code, raw: response[:body]}

          if status == 429 || code == REFRESH_QUOTA_EXCEEDED_ERROR
            raise Etlify::RateLimited.new(message, retry_after: REFRESH_RETRY_AFTER, **options)
          end

          klass = status >= 500 ? Etlify::ApiError : Etlify::Unauthorized
          raise klass.new(message, **options)
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
