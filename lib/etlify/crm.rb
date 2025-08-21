# frozen_string_literal: true

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
      # Etlify::CRM.register(:my_crm, adapter: MyAdapter, options: { job_class: X })
      def register(name, adapter:, options: {})
        key = name.to_sym
        registry[key] = RegistryItem.new(
          name: key,
          adapter: adapter,
          options: options || {}
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
    end
  end
end
