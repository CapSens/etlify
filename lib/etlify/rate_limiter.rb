module Etlify
  class RateLimiter
    attr_reader :interval

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
      @last_call_at = monotonic_now
    end

    def self.null
      NullLimiter.new
    end

    private

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
