module Etlify
  module Adapters
    module Dynamics365
      # Thread-safe in-memory cache for the Dataverse OAuth
      # bearer token. The cached entry expires
      # SAFETY_MARGIN_SECONDS before its declared TTL to
      # avoid race conditions with the IdP clock.
      class TokenCache
        SAFETY_MARGIN_SECONDS = 60

        # @param clock [#call] returns the current Time
        def initialize(clock: Time.method(:now))
          @clock = clock
          @mutex = Mutex.new
          @entry = nil
        end

        # @yieldreturn [Hash{Symbol => Object}] {token: String, expires_at: Time}
        # @return [String] the cached or freshly-fetched token
        def fetch
          @mutex.synchronize do
            return @entry[:token] if cached_entry_valid?

            fresh = yield
            validate_entry!(fresh)
            @entry = fresh
            @entry[:token]
          end
        end

        def invalidate!
          @mutex.synchronize { @entry = nil }
        end

        private

        def cached_entry_valid?
          return false unless @entry.is_a?(Hash)
          return false unless @entry[:token].is_a?(String)
          return false unless @entry[:expires_at].is_a?(Time)

          @entry[:expires_at] > @clock.call + SAFETY_MARGIN_SECONDS
        end

        def validate_entry!(entry)
          return if valid_entry?(entry)

          raise ArgumentError,
                "TokenCache block must return {token: String, expires_at: Time}"
        end

        def valid_entry?(entry)
          entry.is_a?(Hash) &&
            entry[:token].is_a?(String) &&
            !entry[:token].empty? &&
            entry[:expires_at].is_a?(Time)
        end
      end
    end
  end
end
