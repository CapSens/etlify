module Etlify
  class RateLimiter
    attr_reader :interval

    # @param max_requests [Numeric] must be > 0 (use NullLimiter for no-op)
    # @param period [Numeric] must be > 0 (use NullLimiter for no-op)
    def initialize(max_requests:, period:)
      unless max_requests.is_a?(Numeric) && max_requests > 0
        raise ArgumentError,
              "max_requests must be positive (use NullLimiter for no-op)"
      end
      unless period.is_a?(Numeric) && period > 0
        raise ArgumentError,
              "period must be positive (use NullLimiter for no-op)"
      end

      @interval = period.to_f / max_requests
    end

    def throttle!
      sleep(@interval)
    end

    class NullLimiter
      def throttle!
      end

      def interval
        0
      end
    end
  end
end
