module Etlify
  module StaleRecords
    # BatchSync: enqueue or perform sync for all stale records discovered by
    # Finder. It only loads full records when running in synchronous mode to
    # keep memory usage low; in async mode it enqueues jobs by id.
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
      def self.call(models: nil,
        crm_name: nil,
        async: true,
        batch_size: DEFAULT_BATCH_SIZE)
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
        stats = {total: 0, per_model: {}, errors: 0}

        # Finder returns: { ModelClass => { crm_sym => relation(ids-only) } }
        Finder.call(models: @models, crm_name: @crm_name).each do |model, per_crm|
          model_count  = 0
          model_errors = 0

          per_crm.each do |crm, relation|
            processed = process_model(model, relation, crm_name: crm)
            model_count  += processed[:count]
            model_errors += processed[:errors]
          end

          stats[:per_model][model.name] = model_count
          stats[:total]  += model_count
          stats[:errors] += model_errors
        end

        stats
      end

      private

      # Process one model's stale relation (ids-only relation) for a given CRM.
      def process_model(model, relation, crm_name:)
        count  = 0
        errors = 0
        pk     = model.primary_key.to_sym

        relation.in_batches(of: @batch_size) do |batch_rel|
          ids = batch_rel.pluck(pk)
          next if ids.empty?

          if @async
            enqueue_async(model, ids, crm_name: crm_name)
            count += ids.size
          else
            # Load full records only when performing inline.
            model.where(pk => ids).find_each(batch_size: @batch_size) do |rec|
              Etlify::Synchronizer.call(rec, crm_name: crm_name)
              count += 1
            rescue
              # Count and continue; no logging by design.
              errors += 1
            end
          end
        end

        {count: count, errors: errors}
      end

      # Enqueue one job per id without loading the records.
      def enqueue_async(model, ids, crm_name:)
        ids.each do |id|
          Etlify::SyncJob.perform_later(model.name, id, crm_name.to_s)
        end
      end
    end
  end
end
