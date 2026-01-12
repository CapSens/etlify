module Etlify
  module StaleRecords
    # Finder builds, for each configured model/CRM, an ids-only relation
    # containing records that are considered "stale" and need to be synced.
    #
    # Return shape:
    #   {
    #     User    => { hubspot: <ActiveRecord::Relation> },
    #     Company => { hubspot: <ActiveRecord::Relation> }
    #   }
    #
    # Each returned relation:
    #   - selects only a single column "id"
    #   - is ordered ASC by id (stable batching)
    class Finder
      class << self
        # Public: Build a nested Hash of:
        #   { ModelClass => { crm_sym => ActiveRecord::Relation(ids only) } }
        #
        # models   - Optional Array of model classes to restrict the search.
        # crm_name - Optional Symbol/String to target a single CRM.
        #
        # Returns a Hash.
        def call(models: nil, crm_name: nil)
          targets = models || etlified_models(crm_name: crm_name)

          targets.each_with_object({}) do |model, out|
            next unless model.table_exists?

            crms = configured_crm_names_for(model, crm_name: crm_name)
            next if crms.empty?

            out[model] = crms.each_with_object({}) do |crm, per_crm|
              per_crm[crm] = stale_relation_for(model, crm_name: crm)
            end
          end
        end

        private

        def etlified_models(crm_name: nil)
          ActiveRecord::Base.descendants.select do |m|
            next false unless m.respond_to?(:table_exists?) && m.table_exists?
            next false unless m.respond_to?(:etlify_crms) && m.etlify_crms.present?

            if crm_name
              m.etlify_crms.key?(crm_name.to_sym)
            else
              m.etlify_crms.any?
            end
          end
        end

        def configured_crm_names_for(model, crm_name: nil)
          return [] unless model.respond_to?(:etlify_crms) && model.etlify_crms

          if crm_name && model.etlify_crms.key?(crm_name.to_sym)
            [crm_name.to_sym]
          else
            model.etlify_crms.keys
          end
        end

        # Call the user-defined stale_scope and ensure proper format.
        def stale_relation_for(model, crm_name:)
          conf = model.etlify_crms.fetch(crm_name)
          stale_scope = conf[:stale_scope]

          relation = stale_scope.call(model, crm_name)

          unless relation.is_a?(ActiveRecord::Relation)
            raise ArgumentError,
                  "stale_scope must return an ActiveRecord::Relation, got #{relation.class}"
          end

          relation.select(:id).reorder(id: :asc)
        end
      end
    end
  end
end
