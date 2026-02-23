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
          sync_line.update!(last_synced_at: Time.current, last_error: nil)
        rescue StandardError
          # no-op
        end
        return :skipped
      end

      result = resource.with_lock do
        # Buffer: if any sync_dependency lacks a crm_id, defer the sync.
        # Placed inside the lock to avoid race conditions.
        if pending_syncs_table_exists? && (missing = missing_sync_dependencies).any?
          buffer_pending_syncs!(missing)
          :buffered
        elsif sync_line.stale?(digest)
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
            last_error: nil
          )

          :synced
        else
          sync_line.update!(last_synced_at: Time.current)
          :not_modified
        end
      end

      # Flush outside the lock to avoid rolling back a successful upsert
      # if the flush fails.
      flush_pending_syncs! if result.in?([:synced, :not_modified])

      result
    rescue => e
      sync_line.update!(last_error: e.message)
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

    # ---------- sync_dependencies: buffer & flush ----------

    # Returns an array of dependency records that do not yet have a crm_id
    # for this CRM.
    def missing_sync_dependencies
      sync_deps = conf[:sync_dependencies]
      return [] if sync_deps.blank?

      sync_deps.filter_map do |assoc_name|
        dep = resource.public_send(assoc_name)
        next unless dep
        next if dependency_has_crm_id?(dep)
        next if cyclic_dependency?(dep)

        dep
      end
    end

    # Check if a dependency already has a CRM id, either via CrmSynchronisation
    # (etlified models) or via a direct column like `airtable_id` (legacy models).
    def dependency_has_crm_id?(dep)
      dep_sync = CrmSynchronisation.find_by(
        resource_type: dep.class.name,
        resource_id: dep.id,
        crm_name: crm_name.to_s
      )
      return true if dep_sync&.crm_id.present?

      # Fallback: check for a direct `#{crm_name}_id` column (legacy models).
      legacy_method = :"#{crm_name}_id"
      dep.respond_to?(legacy_method) && dep.send(legacy_method).present?
    end

    # Detect cyclic dependencies: if the dependency is already waiting on
    # the current resource, skip buffering to avoid infinite loops.
    def cyclic_dependency?(dep)
      return false unless pending_syncs_table_exists?

      Etlify::PendingSync.exists?(
        dependent_type: dep.class.name,
        dependent_id: dep.id,
        dependency_type: resource.class.name,
        dependency_id: resource.id,
        crm_name: crm_name.to_s
      )
    end

    # Create PendingSync rows for each missing dependency and enqueue the
    # dependency for sync (so it gets its crm_id).
    def buffer_pending_syncs!(missing_deps)
      missing_deps.each do |dep|
        Etlify::PendingSync.find_or_create_by!(
          dependent_type: resource.class.name,
          dependent_id: resource.id,
          dependency_type: dep.class.name,
          dependency_id: dep.id,
          crm_name: crm_name.to_s
        )

        # Enqueue the dependency so it gets synced and obtains a crm_id.
        dep.crm_sync!(crm_name: crm_name) if dep.respond_to?(:crm_sync!)
      end
    end

    # After a successful sync, find all PendingSync rows where this resource
    # was the dependency and re-enqueue the dependents.
    def flush_pending_syncs!
      return unless pending_syncs_table_exists?

      pending = Etlify::PendingSync.for_dependency(resource, crm_name: crm_name)
      return if pending.empty?

      # Snapshot IDs to avoid deleting records created between select and delete.
      pending_ids = pending.pluck(:id)

      pending.find_each do |ps|
        dependent = ps.dependent_type.constantize.find_by(id: ps.dependent_id)
        dependent&.crm_sync!(crm_name: ps.crm_name.to_sym) if dependent&.respond_to?(:crm_sync!)
      end

      Etlify::PendingSync.where(id: pending_ids).delete_all
    end

    def pending_syncs_table_exists?
      return @pending_syncs_table_exists if defined?(@pending_syncs_table_exists)

      @pending_syncs_table_exists = begin
        ActiveRecord::Base.connection.data_source_exists?("etlify_pending_syncs")
      rescue StandardError
        false
      end
    end
  end
end
