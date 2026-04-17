module Etlify
  module StaleRecords
    # BatchSync: enqueue or perform sync for all stale records discovered by
    # Finder. In async mode it enqueues a single BatchSyncJob per CRM;
    # in sync mode it loads records and syncs inline.
    class BatchSync
      DEFAULT_BATCH_SIZE = 1_000

      # Public: Run a batch sync over all stale records.
      #
      # models:     Optional Array<Class> to restrict scanned models.
      # crm_name:   Optional Symbol/String; restrict processing to this CRM.
      # async:      true => enqueue jobs; false => perform inline.
      # batch_size: # of ids per batch.
      #
      # Returns a Hash with :total, :per_model, :errors.
      def self.call(
        models: nil,
        crm_name: nil,
        async: true,
        batch_size: DEFAULT_BATCH_SIZE
      )
        new(
          models: models,
          crm_name: crm_name,
          async: async,
          batch_size: batch_size
        ).call
      end

      def initialize(models:, crm_name:, async:, batch_size:)
        @models     = models
        @crm_name   = crm_name&.to_sym
        @async      = !!async
        @batch_size = Integer(batch_size)
      end

      def call
        if @async
          call_async
        else
          call_sync
        end
      end

      private

      def call_async
        stats = {total: 0, per_model: {}, errors: 0}
        pending_pairs = Hash.new { |h, k| h[k] = [] }

        stale_results.each do |model, per_crm|
          model_count = 0

          per_crm.each do |crm, relation|
            next unless Etlify::CRM.enabled?(crm)

            relation.ids.each { |id| pending_pairs[crm] << [model.name, id] }
            model_count += relation.ids.size
          end

          stats[:per_model][model.name] = model_count
          stats[:total] += model_count
        end

        enqueue_batch_jobs(pending_pairs)
        stats
      end

      def call_sync
        stats = {total: 0, per_model: {}, errors: 0}

        stale_results.each do |model, per_crm|
          model_count  = 0
          model_errors = 0

          per_crm.each do |crm, relation|
            next unless Etlify::CRM.enabled?(crm)

            processed = process_model_sync(model, relation, crm_name: crm)
            model_count  += processed[:count]
            model_errors += processed[:errors]
          end

          stats[:per_model][model.name] = model_count
          stats[:total]  += model_count
          stats[:errors] += model_errors
        end

        stats
      end

      def stale_results
        Finder.call(models: @models, crm_name: @crm_name)
      end

      # Process one model's stale relation inline (sync mode).
      def process_model_sync(model, relation, crm_name:)
        count  = 0
        errors = 0
        primary_key = model.primary_key.to_sym

        model.unscoped
             .where(primary_key => relation)
             .find_each(batch_size: @batch_size) do |record|
          conf  = record.class.etlify_crms.fetch(crm_name.to_sym)
          guard = conf[:guard]
          next if guard && !guard.call(record)

          service = Etlify::Synchronizer.call(record, crm_name: crm_name)
          count += 1
          errors += 1 if service == :error
        end

        {count: count, errors: errors}
      end

      # Enqueue one BatchSyncJob per CRM with all collected pairs.
      def enqueue_batch_jobs(pending_pairs)
        pending_pairs.each do |crm, pairs|
          next if pairs.empty?

          job_class = job_class_for(crm)
          flat_pairs = pairs.flatten
          job_class.perform_later(crm.to_s, flat_pairs)
        end
      end

      # Returns the job class to use for enqueuing batch sync jobs.
      # Uses the custom job_class from CRM options if defined,
      # otherwise falls back to Etlify::BatchSyncJob.
      def job_class_for(crm_name)
        crm_config = Etlify::CRM.registry[crm_name.to_sym]
        return Etlify::BatchSyncJob unless crm_config

        custom_class = crm_config.options[:job_class]
        return Etlify::BatchSyncJob unless custom_class

        custom_class.constantize
      end
    end
  end
end
