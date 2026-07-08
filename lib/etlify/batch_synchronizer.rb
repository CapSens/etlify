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
      unless Etlify::CRM.enabled?(@crm_name)
        return {
          synced: 0,
          skipped: @records.size,
          buffered: 0,
          not_modified: 0,
          errors: 0,
          disabled: true,
        }
      end

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
        stats[:synced] += sync_results[:synced]
        stats[:errors] += sync_results[:errors]
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

      # Resolve the matching value. A blank value is only acceptable when
      # the record already has a crm_id (update by id, no matching needed):
      # without one, the record can neither be reconciled nor created with
      # its unique property, so fail it explicitly instead of guessing.
      # A raising match_by proc is isolated the same way so one bad record
      # never blocks the whole batch.
      begin
        match_value = Etlify::MatchBy.resolve(record, @conf)
        if match_value.empty? && sync_line.crm_id.blank?
          raise ArgumentError,
                "match_by value resolved blank and no crm_id is known"
        end
      rescue => e
        bump_error!({sync_line: sync_line}, e)
        return [:errors, nil]
      end

      item = {
        record: record,
        payload: payload,
        digest: digest,
        sync_line: sync_line,
        match_value: match_value,
      }
      [:ready, item]
    end

    # Split ready records by whether they already have a crm_id:
    #   - with crm_id    -> update directly by crm_id (batch_update!)
    #   - without crm_id -> reconcile/create via match_by (batch_upsert!)
    # Each group is processed and recovered independently so a failure in one
    # never re-processes the other.
    def perform_batch_upsert!(ready_items)
      with_crm_id, without_crm_id =
        ready_items.partition { |item| item[:sync_line].crm_id.present? }

      stats = {synced: 0, errors: 0}
      merge_stats!(stats, update_existing_batch!(with_crm_id)) if with_crm_id.any?
      merge_stats!(stats, upsert_new_batch!(without_crm_id)) if without_crm_id.any?
      stats
    end

    # Records that already have a crm_id are updated by crm_id, so a changed
    # matching value (e.g. email) can no longer trigger a duplicate/collision.
    # The payload is sent as-is: the matching property is never written
    # unless the serializer explicitly includes it.
    def update_existing_batch!(items)
      records = items.map do |item|
        {crm_id: item[:sync_line].crm_id, properties: item[:payload]}
      end

      @adapter.batch_update!(
        object_type: @conf[:crm_object_type],
        records: records
      )

      apply_batch_results!(items) { |item| item[:sync_line].crm_id }
    rescue Etlify::RateLimited
      # Always bubble up so BatchSyncJob can re-enqueue with backoff.
      raise
    rescue Etlify::Error => e
      raise unless per_record_error?(e)

      fallback_to_sequential_upsert!(items)
    end

    # First-time sync (no crm_id yet): reconcile through match_by using the
    # CRM's native batch upsert endpoint, then fall back to sequential upserts
    # to isolate the offending record on a deterministic per-record error.
    def upsert_new_batch!(items)
      inputs = items.map do |item|
        {value: item[:match_value], properties: item[:payload]}
      end

      crm_id_mapping = @adapter.batch_upsert!(
        object_type: @conf[:crm_object_type],
        inputs: inputs,
        match_property: Etlify::MatchBy.property(@conf)
      )

      apply_batch_results!(items) do |item|
        crm_id = crm_id_mapping[item[:match_value]]
        if crm_id.blank?
          # A missing mapping entry means the CRM never confirmed this
          # record: marking it synced would persist crm_id nil with a fresh
          # digest and the record would never be retried. Fail it explicitly
          # (apply_batch_results! bumps error_count per item) so it stays
          # stale and visible.
          message = [
            "batch_upsert! returned no crm_id for match value",
            item[:match_value].inspect,
          ].join(" ")
          raise Etlify::SyncError, message
        end

        crm_id
      end
    rescue Etlify::RateLimited
      raise
    rescue Etlify::Error => e
      raise unless per_record_error?(e)

      # Batch endpoints are atomic: a single bad record (duplicate on merge
      # field, unique-property collision, dead reference, ...) fails the whole
      # batch. Without this fallback the batch is stuck forever (same args,
      # same failure) and error_count is never bumped. The sequential loop
      # isolates the offender so healthy records sync and only the bad one
      # bumps error_count, letting the Finder exclude it via max_sync_errors.
      fallback_to_sequential_upsert!(items)
    end

    def fallback_to_sequential_upsert!(items)
      synced = 0
      errors = 0
      now = Time.current

      items.each do |item|
        crm_id = @adapter.upsert!(
          payload: item[:payload],
          match_property: Etlify::MatchBy.property(@conf),
          match_value: item[:match_value],
          object_type: @conf[:crm_object_type],
          crm_id: item[:sync_line].crm_id
        )

        mark_synced!(item, crm_id, now)
        synced += 1
      rescue Etlify::RateLimited
        raise
      rescue => e
        errors += 1
        bump_error!(item, e)
      end

      {synced: synced, errors: errors}
    end

    # Persist results of a native batch call. The block returns the crm_id for
    # a given item (the existing one for updates, the mapped one for upserts).
    def apply_batch_results!(items)
      synced = 0
      errors = 0
      now = Time.current

      items.each do |item|
        mark_synced!(item, yield(item), now)
        synced += 1
      rescue Etlify::RateLimited
        raise
      rescue => e
        errors += 1
        bump_error!(item, e)
      end

      {synced: synced, errors: errors}
    end

    def mark_synced!(item, crm_id, now)
      item[:sync_line].update!(
        crm_name: @crm_name,
        crm_id: crm_id.presence || item[:sync_line].crm_id,
        last_digest: item[:digest],
        last_synced_at: now,
        last_error: nil,
        error_count: 0
      )

      flush_pending_syncs!(item[:record])
    end

    def bump_error!(item, error)
      item[:sync_line].update!(
        last_error: error.message,
        error_count: item[:sync_line].error_count.to_i + 1
      )
    rescue
      # no-op
    end

    def merge_stats!(acc, partial)
      acc[:synced] += partial[:synced]
      acc[:errors] += partial[:errors]
      acc
    end

    # A deterministic, per-record CRM error worth isolating via the sequential
    # fallback: validation failures and other 4xx (e.g. HubSpot's 400 on a
    # unique-property collision), but never auth (401/403) or rate limit (429)
    # — those must bubble up. 5xx / transport errors also bubble (job retry).
    def per_record_error?(error)
      return true if error.is_a?(Etlify::ValidationFailed)
      return false unless error.is_a?(Etlify::ApiError)

      status = error.status.to_i
      (400..499).cover?(status) && ![401, 403, 429].include?(status)
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
