module Etlify
  # Throttles the HTTP calls an adapter makes, to stay within a CRM's rate
  # limit.
  #
  # Two modes:
  #
  # - **Shared bucket** (default, whenever a cache store is available): each
  #   call atomically claims a slot in a fixed window stored in the cache, so
  #   every thread and every process draws on the same budget. This is the only
  #   mode that holds when several workers sync concurrently.
  # - **Local pacing** (`cache: false`, or when the store cannot answer): sleeps
  #   `period / max_requests` before each call. Paces a single thread correctly,
  #   but N concurrent workers then issue up to N times the configured rate.
  #
  # The shared bucket needs a store shared across processes (Redis, Memcached)
  # to bound the global rate. With a per-process `MemoryStore` it still bounds
  # each process, which is stricter than local pacing but not global.
  class RateLimiter
    DEFAULT_KEY = "etlify:rate_limit".freeze

    # Upper bound on how many windows a single call may wait for budget. Past
    # that the call proceeds, paced locally: a limit configured far below the
    # actual workload must never hold a worker thread hostage indefinitely.
    MAX_WINDOW_WAITS = 10

    attr_reader(
      :interval,
      :key,
      :max_requests,
      :period
    )

    # @param max_requests [Numeric] slots allowed per period, must be > 0
    # @param period [Numeric] window length in seconds, must be > 0
    # @param cache [ActiveSupport::Cache::Store, false, nil] store backing the
    #   shared bucket. `false` or `nil` falls back to local pacing.
    # @param key [String, nil] cache key namespace, scoped per CRM by the
    #   registry so two CRMs never share a budget.
    def initialize(max_requests:, period:, cache: nil, key: nil)
      cache = nil if cache == false

      validate_max_requests!(max_requests)
      validate_period!(period)
      validate_cache!(cache)

      @max_requests = max_requests
      @period = period.to_f
      @interval = @period / max_requests
      @cache = cache
      @key = key || DEFAULT_KEY
    end

    # Whether the budget is shared across threads and processes.
    def shared?
      !@cache.nil?
    end

    # Block until this call fits within the configured rate.
    def throttle!
      return sleep(@interval) unless shared?

      MAX_WINDOW_WAITS.times do
        now = current_time
        claimed = claim_slot(now)

        # The store could not answer (NullStore, a store without #increment,
        # or a Redis failsafe swallowing a connection error): fall back to
        # local pacing rather than letting the call through unthrottled.
        return sleep(@interval) if claimed.nil?
        return nil if claimed <= @max_requests

        sleep(seconds_until_next_window(now))
      end

      sleep(@interval)
    end

    class NullLimiter
      def throttle!
      end

      def interval
        0
      end

      def shared?
        false
      end
    end

    private

    # Atomically claim a slot in the window covering `now`.
    #
    # Returns the number of slots claimed in that window so far, or nil when
    # the store cannot answer. A rejected call still counts, which is harmless:
    # the counter lives and dies with its window.
    def claim_slot(now)
      @cache.increment(
        window_key(now),
        1,
        expires_in: (@period * 2).ceil
      )
    rescue
      nil
    end

    # Windows are derived from wall-clock time so that separate processes
    # agree on the current window without coordinating.
    def window_key(now)
      "#{@key}:#{(now / @period).floor}"
    end

    def seconds_until_next_window(now)
      remaining = @period - (now % @period)

      # Spread the wake-ups of everyone waiting on the same boundary, so they
      # do not all race for the next window's first slots.
      remaining + (rand * @interval)
    end

    def current_time
      Process.clock_gettime(Process::CLOCK_REALTIME)
    end

    def validate_max_requests!(max_requests)
      return if max_requests.is_a?(Numeric) && max_requests > 0

      raise(
        ArgumentError,
        "max_requests must be positive (use NullLimiter for no-op)"
      )
    end

    def validate_period!(period)
      return if period.is_a?(Numeric) && period > 0

      raise(
        ArgumentError,
        "period must be positive (use NullLimiter for no-op)"
      )
    end

    def validate_cache!(cache)
      return if cache.nil?
      return if cache.respond_to?(:increment)

      message = [
        "cache must respond to #increment (got #{cache.class});",
        "pass cache: false to disable the shared bucket",
      ].join(" ")

      raise(ArgumentError, message)
    end
  end
end
