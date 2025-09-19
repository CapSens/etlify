# lib/etlify/model.rb
module Etlify
  module Model
    extend ActiveSupport::Concern

    included do
      # Track including classes for DSL backfill.
      Etlify::Model.__included_klasses__ << self

      # Per-CRM configuration storage.
      class_attribute :etlify_crms, instance_writer: false, default: {}

      # Declare associations only for ActiveRecord models.
      if defined?(ActiveRecord::Base) && self < ActiveRecord::Base
        if respond_to?(:reflect_on_association) &&
            !reflect_on_association(:crm_synchronisations)
          has_many(
            :crm_synchronisations,
            -> { order(id: :asc) },
            class_name: "CrmSynchronisation",
            as: :resource,
            dependent: :destroy,
            inverse_of: :resource
          )
        end
      end

      # Install DSL and instance helpers for already-registered CRMs.
      Etlify::CRM.names.each do |crm_name|
        Etlify::Model.define_crm_dsl_on(self, crm_name)
        Etlify::Model.define_crm_instance_helpers_on(self, crm_name)
      end
    end

    class << self
      def __included_klasses__
        @__included_klasses__ ||= []
      end

      def install_dsl_for_crm(crm_name)
        __included_klasses__.each do |klass|
          define_crm_dsl_on(klass, crm_name)
          define_crm_instance_helpers_on(klass, crm_name)
        end
      end

      def define_crm_dsl_on(klass, crm_name)
        dsl_name = "#{crm_name}_etlified_with"
        return if klass.respond_to?(dsl_name)

        klass.define_singleton_method(dsl_name) do |
          serializer:,
          crm_object_type:,
          id_property:,
          dependencies: [],
          sync_if: ->(_r) { true },
          job_class: nil
        |
          reg = Etlify::CRM.fetch(crm_name)

          conf = {
            serializer: serializer,
            guard: sync_if,
            crm_object_type: crm_object_type,
            id_property: id_property,
            dependencies: Array(dependencies).map(&:to_sym),
            adapter: reg.adapter,
            job_class: job_class || reg.options[:job_class],
          }

          new_hash = (etlify_crms || {}).dup
          new_hash[crm_name.to_sym] = conf
          self.etlify_crms = new_hash

          Etlify::Model.define_crm_instance_helpers_on(self, crm_name)
        end
      end

      def define_crm_instance_helpers_on(klass, crm_name)
        payload_m = "#{crm_name}_build_payload"
        sync_m    = "#{crm_name}_sync!"
        delete_m  = "#{crm_name}_delete!"

        unless klass.method_defined?(payload_m)
          klass.define_method(payload_m) do
            build_crm_payload(crm_name: crm_name)
          end
        end

        unless klass.method_defined?(sync_m)
          klass.define_method(sync_m) do |async: true, job_class: nil|
            crm_sync!(crm_name: crm_name, async: async, job_class: job_class)
          end
        end

        unless klass.method_defined?(delete_m)
          klass.define_method(delete_m) do
            crm_delete!(crm_name: crm_name)
          end
        end

        unless klass.method_defined?("registered_crms")
          klass.define_method("registered_crms") do
            self.class.etlify_crms.keys.map(&:to_s)
          end
        end

        # Define filtered has_one only for AR models.
        if defined?(ActiveRecord::Base) && klass < ActiveRecord::Base
          assoc_name = :"#{crm_name}_crm_synchronisation"
          if klass.respond_to?(:reflect_on_association) &&
              !klass.reflect_on_association(assoc_name)
            klass.has_one(
              assoc_name,
              -> { where(crm_name: crm_name.to_s) },
              class_name: "CrmSynchronisation",
              as: :resource,
              dependent: :destroy,
              inverse_of: :resource
            )
          end
        end
      end
    end

    # ---------- Public generic API (CRM-aware) ----------

    def crm_synced?(crm_name: nil)
      respond_to?(:crm_synchronisation) && crm_synchronisation.present?
    end

    def build_crm_payload(crm_name: nil)
      raise ArgumentError, "crm_name is required" if crm_name.nil?

      raise_unless_crm_is_configured(crm_name)

      conf = self.class.etlify_crms.fetch(crm_name.to_sym)
      serializer = conf[:serializer].new(self)

      if serializer.respond_to?(:as_crm_payload)
        serializer.as_crm_payload
      elsif serializer.respond_to?(:to_h)
        serializer.to_h
      else
        raise ArgumentError, "Serializer must implement as_crm_payload"
      end
    end

    def crm_sync!(crm_name: nil, async: true, job_class: nil)
      raise ArgumentError, "crm_name is required" if crm_name.nil?
      return false unless allow_sync_for?(crm_name)

      if async
        jc = resolve_job_class_for(crm_name, override: job_class)
        if jc.respond_to?(:perform_later)
          jc.perform_later(self.class.name, id, crm_name.to_s)
        elsif jc.respond_to?(:perform_async)
          jc.perform_async(self.class.name, id, crm_name.to_s)
        else
          raise ArgumentError, "No job class available for CRM sync"
        end
      else
        Etlify::Synchronizer.call(self, crm_name: crm_name)
      end
    end

    def crm_delete!(crm_name: nil)
      raise ArgumentError, "crm_name is required" if crm_name.nil?

      Etlify::Deleter.call(self, crm_name: crm_name)
    end

    private

    def allow_sync_for?(crm_name)
      conf = self.class.etlify_crms[crm_name.to_sym]
      return false unless conf

      guard = conf[:guard]
      guard ? guard.call(self) : true
    end

    def resolve_job_class_for(crm_name, override:)
      return constantize_if_needed(override) if override

      conf = self.class.etlify_crms.fetch(crm_name.to_sym)
      given = conf[:job_class]
      return constantize_if_needed(given) if given

      constantize_if_needed("Etlify::SyncJob")
    end

    def constantize_if_needed(klass_or_name)
      return klass_or_name unless klass_or_name.is_a?(String)

      klass_or_name.constantize
    end

    def raise_unless_crm_is_configured(crm_name)
      unless self.class.etlify_crms && self.class.etlify_crms[crm_name.to_sym]
        raise ArgumentError, "crm not configured for #{crm_name}"
      end
    end
  end
end
