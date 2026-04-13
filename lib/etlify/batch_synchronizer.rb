module Etlify
  # Batch-aware synchronizer: applies per-record pre-checks (guard,
  # digest, dependencies) then calls adapter.batch_upsert! for all
  # records that are ready, and updates sync_lines in bulk.
  #
  # All records MUST belong to the same model class.
  class BatchSynchronizer
    # @param records [Array<ActiveRecord::Base>] same-model records
    # @param crm_name [Symbol, String]
    # @return [Hash] stats {synced:, skipped:, buffered:, not_modified:, errors:}
    def self.call(records, crm_name:)
      new(records, crm_name: crm_name).call
    end

    def initialize(records, crm_name:)
      @records  = records
      @crm_name = crm_name.to_sym
      @klass    = records.first.class
      @conf     = @klass.etlify_crms.fetch(@crm_name)
      @adapter  = @conf[:adapter]
    end

    def call
      stats = {synced: 0, skipped: 0, buffered: 0, not_modified: 0, errors: 0}
      ready = []

      @records.each do |record|
        status, item = prepare_record(record)
        if status == :ready
          ready << item
        else
          stats[status] += 1
        end
      end

      if ready.any?
        sync_results = perform_batch_upsert!(ready)
        stats[:synced] = sync_results[:synced]
        stats[:errors] = sync_results[:errors]
      end

      stats
    end

    private

    def prepare_record(record)
      sync_line = record.crm_synchronisations
                        .find_or_initialize_by(crm_name: @crm_name)

      # Guard check
      guard = @conf[:guard]
      unless guard.nil? || guard.call(record)
        begin
          sync_line.update!(
            last_synced_at: Time.current,
            last_error: nil,
            error_count: 0
          )
        rescue
          # no-op
        end
        return [:skipped, nil]
      end

      # Dependency check (delegate to Synchronizer for complex logic)
      if missing_sync_dependencies?(record)
        buffer_pending_syncs!(record)
        return [:buffered, nil]
      end

      # Build payload + digest
      payload = record.build_crm_payload(crm_name: @crm_name)
      digest  = Etlify.config.digest_strategy.call(payload)

      # Stale check
      unless sync_line.stale?(digest)
        sync_line.update!(last_synced_at: Time.current)
        return [:not_modified, nil]
      end

      item = {
        record: record,
        payload: payload,
        digest: digest,
        sync_line: sync_line,
      }
      [:ready, item]
    end

    def perform_batch_upsert!(ready_items)
      payloads = ready_items.map { |item| item[:payload] }
      id_prop = @conf[:id_property].to_s

      # Let RateLimited bubble up to the caller (BatchSyncJob)
      # so it can re-enqueue with remaining records.
      crm_id_mapping = @adapter.batch_upsert!(
        object_type: @conf[:crm_object_type],
        records: payloads,
        id_property: id_prop
      )

      synced = 0
      errors = 0
      now = Time.current

      ready_items.each do |item|
        id_value = extract_id_value(item[:payload], id_prop)
        crm_id = crm_id_mapping[id_value]

        item[:sync_line].update!(
          crm_name: @crm_name,
          crm_id: crm_id.presence || item[:sync_line].crm_id,
          last_digest: item[:digest],
          last_synced_at: now,
          last_error: nil,
          error_count: 0
        )
        synced += 1

        flush_pending_syncs!(item[:record])
      rescue => e
        errors += 1
        begin
          item[:sync_line].update!(
            last_error: e.message,
            error_count: item[:sync_line].error_count.to_i + 1
          )
        rescue
          # no-op
        end
      end

      {synced: synced, errors: errors}
    end

    def extract_id_value(payload, id_prop)
      (payload[id_prop] || payload[id_prop.to_sym] || "").to_s
    end

    # --- Dependency helpers ---

    def missing_sync_dependencies?(record)
      sync_deps = @conf[:sync_dependencies]
      return false if sync_deps.blank?

      sync_deps.any? do |assoc_name|
        dep = record.public_send(assoc_name)
        next false unless dep
        next false if dependency_has_crm_id?(dep)
        next false if cyclic_dependency?(record, dep)

        true
      end
    end

    def dependency_has_crm_id?(dep)
      dep_sync = CrmSynchronisation.find_by(
        resource_type: dep.class.name,
        resource_id: dep.id,
        crm_name: @crm_name.to_s
      )
      return true if dep_sync&.crm_id.present?

      legacy_method = :"#{@crm_name}_id"
      dep.respond_to?(legacy_method) && dep.send(legacy_method).present?
    end

    def cyclic_dependency?(record, dep)
      return false unless pending_syncs_table_exists?

      Etlify::PendingSync.exists?(
        dependent_type: dep.class.name,
        dependent_id: dep.id,
        dependency_type: record.class.name,
        dependency_id: record.id,
        crm_name: @crm_name.to_s
      )
    end

    def buffer_pending_syncs!(record)
      sync_deps = @conf[:sync_dependencies] || []
      sync_deps.each do |assoc_name|
        dep = record.public_send(assoc_name)
        next unless dep
        next if dependency_has_crm_id?(dep)
        next if cyclic_dependency?(record, dep)

        Etlify::PendingSync.find_or_create_by!(
          dependent_type: record.class.name,
          dependent_id: record.id,
          dependency_type: dep.class.name,
          dependency_id: dep.id,
          crm_name: @crm_name.to_s
        )

        dep.crm_sync!(crm_name: @crm_name) if dep.respond_to?(:crm_sync!)
      end
    end

    def flush_pending_syncs!(record)
      return unless pending_syncs_table_exists?

      pending = Etlify::PendingSync.for_dependency(record, crm_name: @crm_name)
      return if pending.empty?

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
      rescue
        false
      end
    end
  end
end
