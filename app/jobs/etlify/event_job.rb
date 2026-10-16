require "digest"
require "json"

module Etlify
  class EventJob < ActiveJob::Base
    queue_as { Etlify.config.job_queue_name }

    ENQUEUE_LOCK_TTL = 15.minutes

    retry_on StandardError, wait: 1.minute, attempts: 3

    around_enqueue do |job, block|
      cache = Etlify.config.cache_store
      key   = self.class.lock_key(*job.arguments)

      locked = if cache.respond_to?(:write)
        cache.write(
          key,
          "1",
          expires_in: ENQUEUE_LOCK_TTL,
          unless_exist: true
        )
      else
        unless cache.exist?(key)
          cache.write(key, "1", expires_in: ENQUEUE_LOCK_TTL)
          true
        end
      end

      block.call if locked
    end

    around_perform do |job, block|
      block.call
    ensure
      cache = Etlify.config.cache_store
      cache.delete(self.class.lock_key(*job.arguments))
    end

    def self.lock_key(model_name, id, crm_name, *rest)
      digest = ::Digest::SHA256.hexdigest(
        JSON.generate([model_name.to_s, id.to_s, crm_name.to_s, *rest])
      )
      "etlify:event_lock:#{model_name}:#{id}:#{crm_name}:#{digest}"
    end

    def perform(
      model_name,
      id,
      crm_name,
      name,
      properties = {},
      contact_properties = {},
      occurred_at = nil,
      sync_first = true
    )
      model  = model_name.constantize
      record = model.find_by(id: id)
      return unless record

      Etlify::EventTracker.call(
        record,
        crm_name: crm_name.to_sym,
        name: name,
        properties: properties,
        contact_properties: contact_properties,
        occurred_at: occurred_at,
        sync_first: sync_first
      )
    end
  end
end
