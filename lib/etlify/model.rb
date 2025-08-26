module Etlify
  module Model
    extend ActiveSupport::Concern

    included do
      # Track classes that included this concern to backfill DSL on register.
      Etlify::Model.__included_klasses__ << self

      # Hash keyed by CRM name, with config per CRM
      class_attribute :etlify_crms, instance_writer: false, default: {}

      Etlify::CRM.names.each do |crm_name|
        Etlify::Model.define_crm_dsl_on(self, crm_name)
        Etlify::Model.define_crm_instance_helpers_on(self, crm_name)
      end
    end

    class << self
      # Internal: store all including classes
      def __included_klasses__
        @__included_klasses__ ||= []
      end

      # Called by Etlify::CRM.register to (re)install DSL on all classes
      def install_dsl_for_crm(crm_name)
        __included_klasses__.each do |klass|
          define_crm_dsl_on(klass, crm_name)
          define_crm_instance_helpers_on(klass, crm_name)
        end
      end

      # Define the class-level DSL method: "<crm>_etlified_with"
      def define_crm_dsl_on(klass, crm_name)
        dsl_name = "#{crm_name}_etlified_with"

        # Avoid redefining if already defined
        return if klass.respond_to?(dsl_name)

        klass.define_singleton_method(dsl_name) do |
          serializer:,
          crm_object_type:,
          id_property:,
          dependencies: [],
          sync_if: ->(_r) { true },
          job_class: nil
        |
          # Fetch registered CRM (adapter, options)
          reg = Etlify::CRM.fetch(crm_name)

          # Merge model-level config for this CRM
          conf = {
            serializer: serializer,
            guard: sync_if,
            crm_object_type: crm_object_type,
            id_property: id_property,
            dependencies: Array(dependencies).map(&:to_sym),
            adapter: reg.adapter,
            # Job class priority: method arg > registry options > nil
            job_class: job_class || reg.options[:job_class],
          }

          # Store into class attribute hash
          new_hash = (etlify_crms || {}).dup
          new_hash[crm_name.to_sym] = conf
          self.etlify_crms = new_hash

          # Ensure instance helpers exist
          Etlify::Model.define_crm_instance_helpers_on(self, crm_name)
        end
      end

      # Define instance helpers: "<crm>_build_payload", "<crm>_sync!",
      # "<crm>_delete!"
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
      end
    end

    # ---------- Public generic API (now CRM-aware) ----------

    def crm_synced?(crm: nil)
      # If you have per-CRM synchronisation records, adapt accordingly.
      # For now keep a single association; adjust when your schema changes.
      crm_synchronisation.present?
    end

    # Accept both crm_name: and crm: for backward compatibility.
    def build_crm_payload(crm_name: nil, crm: nil)
      crm_name ||= crm
      raise ArgumentError, "crm_name is required" if crm_name.nil?

      raise_unless_crm_is_configured(crm_name)

      conf = self.class.etlify_crms.fetch(crm_name.to_sym)
      conf[:serializer].new(self).as_crm_payload
    end

    # @param crm_name [Symbol] which CRM to use
    # @param async [Boolean] whether to enqueue or run inline
    # @param job_class [Class,String,nil] explicit override
    #
    # Accept both crm_name: and crm: for backward compatibility.
    def crm_sync!(crm_name: nil, async: true, job_class: nil, crm: nil)
      crm_name ||= crm
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

    # Accept both crm_name: and crm: for backward compatibility.
    def crm_delete!(crm_name: nil, crm: nil)
      crm_name ||= crm
      raise ArgumentError, "crm_name is required" if crm_name.nil?

      Etlify::Deleter.call(self, crm_name: crm_name)
    end

    private

    # Guard evaluation per CRM
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

      # Fallback to default sync job name if you want one
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
