module Etlify
  module CRM
    RegistryItem = Struct.new(
      :name,
      :adapter,
      :options,
      keyword_init: true
    )

    class << self
      # Holds { Symbol => RegistryItem }
      def registry
        @registry ||= {}
      end

      # Public API: register a new CRM
      # Etlify::CRM.register(:my_crm, adapter: MyAdapter.new, options: { job_class: X })
      def register(name, adapter:, options: {})
        key = name.to_sym

        if adapter.is_a?(Class)
          raise(
            ArgumentError,
            "Adapter must be an instance, not a class"
          )
        end

        validate_rate_limit!(options[:rate_limit]) if options[:rate_limit]

        copied_options =
          if options
            options.respond_to?(:deep_dup) ? options.deep_dup : options.dup
          else
            {}
          end
        copied_options.freeze

        registry[key] = RegistryItem.new(
          name: key,
          adapter: adapter,
          options: copied_options
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

      private

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
