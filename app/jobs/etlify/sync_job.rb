# app/jobs/etlify/sync_job.rb
module Etlify
  class SyncJob < ActiveJob::Base
    # Queue name is configurable
    queue_as { Etlify.config.job_queue_name }

    # Keep TTL >= 15 minutes to match specs using travel 16.minutes
    ENQUEUE_LOCK_TTL = 15.minutes

    # Retry and schedule a new run on StandardError
    # The TestAdapter preserves :at for scheduled jobs in the queue.
    retry_on StandardError, wait: 1.minute, attempts: 3

    # Deduplicate enqueues per (model, id, crm_name).
    around_enqueue do |job, block|
      cache = Etlify.config.cache_store
      key   = enqueue_lock_key(job.arguments)

      # Prefer atomic write-if-absent when cache supports it.
      locked = if cache.respond_to?(:write)
        cache.write(
          key,
          "1",
          expires_in: ENQUEUE_LOCK_TTL,
          unless_exist: true
        )
      else
        # Fallback: check then write (non-atomic, acceptable for tests).
        unless cache.exist?(key)
          cache.write(key, "1", expires_in: ENQUEUE_LOCK_TTL)
          true
        end
      end

      block.call if locked
    end

    # Always clear the lock after the execution attempt, success or failure.
    # When a retry is scheduled, around_enqueue will set the lock again.
    around_perform do |job, block|
      block.call
    ensure
      cache = Etlify.config.cache_store
      cache.delete(enqueue_lock_key(job.arguments))
    end

    # Perform the CRM sync for the given record.
    def perform(model_name, id, crm_name)
      model  = model_name.constantize
      record = model.find_by(id: id)
      return unless record

      Etlify::Synchronizer.call(record, crm_name: crm_name.to_sym)
    end

    private

    # Lock key includes the CRM name (v2) to allow one job per CRM.
    def enqueue_lock_key(args)
      model_name, id, crm_name = args
      "etlify:enqueue_lock:v2:#{model_name}:#{id}:#{crm_name}"
    end
  end
end
