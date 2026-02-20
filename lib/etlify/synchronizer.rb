module Etlify
  class Synchronizer
    attr_accessor(
      :adapter,
      :conf,
      :crm_name,
      :resource
    )

    # Main entry point (CRM-aware).
    # @param resource [ActiveRecord::Base]
    # @param crm_name [Symbol,String]
    def self.call(resource, crm_name:)
      new(resource, crm_name: crm_name).call
    end

    def initialize(resource, crm_name:)
      @resource = resource
      @crm_name = crm_name.to_sym
      @conf     = resource.class.etlify_crms.fetch(@crm_name)
      @adapter  = @conf[:adapter]

      unless @adapter.is_a?(Object) && @adapter.respond_to?(:upsert!)
        raise ArgumentError, "Adapter must be an instance responding to upsert!"
      end
    end

    def call
      # Honor sync guard first. If guard returns false, skip the sync.
      guard = conf[:guard]
      unless guard.nil? || guard.call(resource)
        # Optionally touch last_synced_at to avoid reprocessing loops.
        # Swallow errors here to keep behavior non-fatal.
        begin
          sync_line.update!(last_synced_at: Time.current, last_error: nil, error_count: 0)
        rescue
          # no-op
        end
        return :skipped
      end

      resource.with_lock do
        if sync_line.stale?(digest)
          crm_id = adapter.upsert!(
            payload: payload,
            id_property: conf[:id_property],
            object_type: conf[:crm_object_type]
          )

          sync_line.update!(
            crm_name: crm_name,
            crm_id: crm_id.presence || sync_line.crm_id,
            last_digest: digest,
            last_synced_at: Time.current,
            last_error: nil,
            error_count: 0
          )
          :synced
        else
          sync_line.update!(last_synced_at: Time.current)
          :not_modified
        end
      end
    rescue => e
      sync_line.update!(
        last_error: e.message,
        error_count: sync_line.error_count.to_i + 1
      )
      :error
    end

    private

    # Compute once to keep idempotency inside the lock.
    def digest
      @digest ||= Etlify.config.digest_strategy.call(payload)
    end

    def payload
      @payload ||= resource.build_crm_payload(crm_name: crm_name)
    end

    # Select or build the per-CRM sync line.
    # If you still have has_one, this keeps working but won't handle multi-CRM.
    def sync_line
      resource.crm_synchronisations.find_or_initialize_by(crm_name: crm_name)
    end
  end
end
