module Etlify
  class RateLimiter
    attr_reader :interval

    # @param max_requests [Numeric] must be > 0 (use NullLimiter for no-op)
    # @param period [Numeric] must be > 0 (use NullLimiter for no-op)
    def initialize(max_requests:, period:)
      raise ArgumentError, "max_requests must be positive" unless max_requests.is_a?(Numeric) && max_requests > 0
      raise ArgumentError, "period must be positive" unless period.is_a?(Numeric) && period > 0

      @interval = period.to_f / max_requests
      @last_call_at = nil
    end

    def throttle!
      if @last_call_at
        elapsed = monotonic_now - @last_call_at
        sleep_time = @interval - elapsed
        sleep(sleep_time) if sleep_time > 0
      end
      # Record when this call happened so the next call
      # can compute the remaining wait time accurately.
      @last_call_at = monotonic_now
    end

    # Convenience factory for a no-op limiter (used in tests
    # or when no throttling is desired).
    def self.null
      NullLimiter.new
    end

    private

    # Monotonic clock is immune to NTP adjustments and system
    # clock changes, making elapsed-time measurements reliable.
    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
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
