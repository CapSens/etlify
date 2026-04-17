module Etlify
  module CRM
    RegistryItem = Struct.new(
      :name,
      :adapter,
      :options,
      :enabled,
      keyword_init: true
    )

    class << self
      # Holds { Symbol => RegistryItem }
      def registry
        @registry ||= {}
      end

      # Public API: register a new CRM
      # Etlify::CRM.register(:my_crm, adapter: MyAdapter.new, options: { job_class: X })
      def register(name, adapter:, enabled: true, options: {})
        key = name.to_sym

        if adapter.is_a?(Class)
          raise(
            ArgumentError,
            "Adapter must be an instance, not a class"
          )
        end

        validate_enabled!(enabled)
        validate_rate_limit!(options[:rate_limit]) if options[:rate_limit]

        copied_options =
          if options
            options.respond_to?(:deep_dup) ? options.deep_dup : options.dup
          else
            {}
          end
        copied_options.freeze

        install_rate_limiter!(adapter, options[:rate_limit])

        registry[key] = RegistryItem.new(
          name: key,
          adapter: adapter,
          options: copied_options,
          enabled: enabled
        )

        # Install DSL on all classes that already included Etlify::Model
        Etlify::Model.install_dsl_for_crm(key)
      end

      # Internal: fetch a RegistryItem
      def fetch(name)
        registry.fetch(name.to_sym)
      end

      # Internal: list all registered CRM names
      def names
        registry.keys
      end

      # Public: whether a CRM is enabled. Unknown CRMs default to true
      # so that this helper is safe to call from any short-circuit site.
      def enabled?(name)
        item = registry[name.to_sym]
        item ? item.enabled : true
      end

      private

      def install_rate_limiter!(adapter, rate_limit)
        return unless rate_limit
        return unless adapter.respond_to?(:rate_limiter=)

        adapter.rate_limiter = Etlify::RateLimiter.new(
          max_requests: rate_limit[:max_requests],
          period: rate_limit[:period]
        )
      end

      def validate_enabled!(enabled)
        return if enabled.is_a?(TrueClass) || enabled.is_a?(FalseClass)

        raise ArgumentError, "enabled must be a boolean (true or false)"
      end

      def validate_rate_limit!(rate_limit)
        unless rate_limit.is_a?(Hash)
          raise ArgumentError, "rate_limit must be a Hash"
        end

        max = rate_limit[:max_requests]
        per = rate_limit[:period]

        unless max.is_a?(Numeric) && max > 0
          raise(
            ArgumentError,
            "rate_limit[:max_requests] must be a positive number"
          )
        end

        unless per.is_a?(Numeric) && per > 0
          raise(
            ArgumentError,
            "rate_limit[:period] must be a positive number"
          )
        end
      end
    end
  end
end
