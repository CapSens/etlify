module Etlify
  module Model
    extend ActiveSupport::Concern

    included do
    end

    class_methods do
      # DSL: etlified_with(
      # serializer:,
      # crm_object_type:,
      # id_property:,
      # sync_if: ->(r){ true }
      # )
      def etlified_with(
        serializer:,
        crm_object_type:,
        dependencies: [],
        id_property:,
        sync_if: ->(_r) { true })
        class_attribute(
          :etlify_serializer,
          instance_accessor: false,
          default: serializer
        )
        class_attribute(
          :etlify_guard,
          instance_accessor: false,
          default: sync_if
        )
        class_attribute(
          :etlify_crm_object_type,
          instance_accessor: true,
          default: crm_object_type
        )
        class_attribute(
          :etlify_id_property,
          instance_accessor: true,
          default: id_property
        )
        class_attribute(
          :etlify_dependencies,
          instance_accessor: false,
          default: Array(dependencies).map(&:to_sym)
        )
        has_one(
          :crm_synchronisation,
          as: :resource,
          dependent: :destroy,
          class_name: "CrmSynchronisation"
        )

      end
    end

    # Public API injected
    def crm_synced?
      crm_synchronisation.present?
    end

    def build_crm_payload
      raise_unless_crm_is_configured

      serializer = self.class.etlify_serializer.new(self)

      if serializer.respond_to?(:as_crm_payload)
        serializer.as_crm_payload
      elsif serializer.respond_to?(:to_h)
        serializer.to_h
      else
        raise NoMethodError, "#{serializer.class} must implement \"as_crm_payload\" or \"to_h\""
      end
    end

    # @param async [Boolean, nil] prioritaire sur la config globale
    def crm_sync!(async: true)
      return false if self.class.respond_to?(:etlify_guard) && !self.class.etlify_guard.call(self)

      if async
        if job_class.respond_to?(:perform_later)
          job_class.perform_later(self.class.name, id)
        elsif job_class.respond_to?(:perform_async)
          job_class.perform_async(self.class.name, id)
        else
          raise ArgumentError, "No job class available for CRM sync"
        end
      else
        Etlify::Synchronizer.call(self)
      end
    end

    def crm_delete!
      Etlify::Deleter.call(self)
    end

    private
      def job_class
        given_class = Etlify.config.sync_job_class
        given_class.is_a?(String) ? given_class.constantize : given_class
      end

      def raise_unless_crm_is_configured
        return if self.class.respond_to?(:etlify_serializer) && self.class.etlify_serializer

        raise ArgumentError, "crm_synced not configured"
      end
  end
end
