module Etlify
  class BatchSyncJob < ActiveJob::Base
    queue_as { Etlify.config.job_queue_name }

    MAX_BATCH_SIZE = 5_000
    LOCK_TTL = 30.minutes
    DEFAULT_RETRY_AFTER = 10

    # Ensure only one batch job per CRM is active at a time.
    around_enqueue do |job, block|
      cache = Etlify.config.cache_store
      key = batch_lock_key(job.arguments)

      locked = if cache.respond_to?(:write)
        cache.write(
          key,
          "1",
          expires_in: LOCK_TTL,
          unless_exist: true
        )
      else
        unless cache.exist?(key)
          cache.write(key, "1", expires_in: LOCK_TTL)
          true
        end
      end

      block.call if locked
    end

    around_perform do |job, block|
      block.call
    ensure
      cache = Etlify.config.cache_store
      cache.delete(batch_lock_key(job.arguments))
    end

    # @param crm_name [String]
    # @param record_pairs [Array, nil] flat array
    #   [model_name, id, model_name, id, ...] or nil for
    #   discovery mode
    def perform(crm_name, record_pairs = nil)
      crm_sym = crm_name.to_sym
      registry_item = Etlify::CRM.fetch(crm_sym)
      adapter = registry_item.adapter

      install_rate_limiter!(adapter, registry_item)

      pairs = if record_pairs
        record_pairs.each_slice(2).to_a
      else
        discover_stale_records(crm_sym)
      end

      process_pairs(pairs, crm_sym)
    ensure
      remove_rate_limiter!(adapter) if adapter
    end

    private

    def discover_stale_records(crm_name)
      pairs = []

      Etlify::StaleRecords::Finder
        .call(crm_name: crm_name)
        .each do |model, per_crm|
          relation = per_crm[crm_name]
          next unless relation

          pk = model.primary_key.to_sym
          scope = model.unscoped.where(pk => relation)
          scope.in_batches(of: MAX_BATCH_SIZE) do |batch_rel|
              batch_rel.pluck(pk).each do |id|
                pairs << [model.name, id]
              end
            end
        end

      pairs
    end

    def process_pairs(pairs, crm_name)
      adapter = Etlify::CRM.fetch(crm_name).adapter

      if adapter.respond_to?(:batch_upsert!)
        process_pairs_batch(pairs, crm_name)
      else
        process_pairs_sequential(pairs, crm_name)
      end
    end

    def process_pairs_batch(pairs, crm_name)
      # Group by model_name to batch per object_type/serializer
      groups = pairs.group_by(&:first)

      processed_models = []
      groups.each do |model_name, model_pairs|
        ids = model_pairs.map(&:last)
        records = model_name.constantize.where(id: ids).to_a
        next if records.empty?

        Etlify::BatchSynchronizer.call(records, crm_name: crm_name)
        processed_models << model_name
      rescue Etlify::RateLimited => e
        remaining = remaining_pairs_from(groups, processed_models, model_name, model_pairs)
        reenqueue(crm_name, remaining, wait: extract_retry_after(e))
        break
      end
    end

    def process_pairs_sequential(pairs, crm_name)
      pairs.each_with_index do |pair, index|
        model_name, id = pair
        record = model_name.constantize.find_by(id: id)
        next unless record

        Etlify::Synchronizer.call(record, crm_name: crm_name)
      rescue Etlify::RateLimited => e
        remaining = pairs[index..]
        wait = extract_retry_after(e)
        reenqueue(crm_name, remaining, wait: wait)
        break
      rescue => _e
        next
      end
    end

    # Build remaining pairs for re-enqueue after a RateLimited error
    # during batch processing. Includes all pairs from unprocessed
    # model groups.
    def remaining_pairs_from(groups, processed_models, current_model, current_pairs)
      remaining = current_pairs.dup
      groups.each do |model_name, model_pairs|
        next if processed_models.include?(model_name)
        next if model_name == current_model

        remaining.concat(model_pairs)
      end
      remaining
    end

    def install_rate_limiter!(adapter, registry_item)
      return unless adapter.respond_to?(:rate_limiter=)

      rate_limit = registry_item.options[:rate_limit]
      return unless rate_limit

      adapter.rate_limiter = Etlify::RateLimiter.new(
        max_requests: rate_limit[:max_requests],
        period: rate_limit[:period]
      )
    end

    def remove_rate_limiter!(adapter)
      return unless adapter.respond_to?(:rate_limiter=)

      adapter.rate_limiter = nil
    end

    def extract_retry_after(error)
      if error.respond_to?(:raw) && error.raw.is_a?(String)
        # HubSpot sometimes includes Retry-After in headers
        # but we only have the body here; use default
      end
      DEFAULT_RETRY_AFTER
    end

    def reenqueue(crm_name, remaining_pairs, wait:)
      cache = Etlify.config.cache_store
      cache.delete(batch_lock_key([crm_name]))

      flat = remaining_pairs.flatten
      self.class.set(wait: wait.seconds)
          .perform_later(crm_name.to_s, flat)
    end

    def batch_lock_key(args)
      crm_name = args.first
      "etlify:batch_sync_lock:#{crm_name}"
    end
  end
end
