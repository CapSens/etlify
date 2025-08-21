module Etlify
  class SyncJob < ActiveJob::Base
    queue_as Etlify.config.job_queue_name
    retry_on(StandardError, attempts: 3, wait: :polynomially_longer)

    ENQUEUE_LOCK_TTL = 15.minutes
    around_enqueue do |job, block|
      key = enqueue_lock_key(job)
      locked = Etlify.config.cache_store.write(
        key,
        1,
        expires_in: ENQUEUE_LOCK_TTL,
        unless_exist: true
      )
      block.call if locked
    end

    around_perform do |job, block|
      block.call
    ensure
      Etlify.config.cache_store.delete(enqueue_lock_key(job))
    end

    def perform(model_class_name, record_id, crm_name)
      model  = model_class_name.constantize
      record = model.find_by(id: record_id)
      return unless record

      Etlify::Synchronizer.call(record, crm: crm_name.to_sym)
    end

    private

    def enqueue_lock_key(job)
      klass, id = job.arguments
      "etlify:jobs:sync:#{klass}:#{id}"
    end
  end
end
