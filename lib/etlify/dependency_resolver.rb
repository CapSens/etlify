# frozen_string_literal: true

module Etlify
  class DependencyResolver
    # Check if all CRM dependencies are satisfied for a resource.
    #
    # @param resource [ActiveRecord::Base]
    # @param crm_name [Symbol]
    # @return [Hash] { satisfied: Boolean, missing_parents: [ActiveRecord::Base] }
    def self.check(resource, crm_name:)
      ensure_table_exists!

      conf = resource.class.etlify_crms.fetch(crm_name.to_sym)
      crm_deps = conf[:crm_dependencies] || []

      return {satisfied: true, missing_parents: []} if crm_deps.empty?

      missing = []

      crm_deps.each do |assoc_name|
        parent = resource.public_send(assoc_name)
        next if parent.nil?

        parent_sync = CrmSynchronisation.find_by(
          crm_name: crm_name.to_s,
          resource_type: parent.class.name,
          resource_id: parent.id
        )

        missing << parent if parent_sync.nil? || parent_sync.crm_id.blank?
      end

      {satisfied: missing.empty?, missing_parents: missing}
    end

    # Register pending dependencies for a resource.
    #
    # @param resource [ActiveRecord::Base]
    # @param crm_name [Symbol]
    # @param missing_parents [Array<ActiveRecord::Base>]
    def self.register_pending!(resource, crm_name:, missing_parents:)
      missing_parents.each do |parent|
        Etlify::SyncDependency.find_or_create_by!(
          crm_name: crm_name.to_s,
          resource_type: resource.class.name,
          resource_id: resource.id,
          parent_resource_type: parent.class.name,
          parent_resource_id: parent.id
        )
      rescue ActiveRecord::RecordNotUnique
        # Already exists, safe to ignore
      end
    end

    # After a parent syncs successfully, find and trigger
    # all pending dependent syncs.
    #
    # @param parent [ActiveRecord::Base]
    # @param crm_name [Symbol]
    def self.resolve_dependents!(parent, crm_name:)
      return unless table_exists?

      pending = Etlify::SyncDependency.pending_for_parent(
        parent,
        crm_name: crm_name
      )

      return if pending.none?

      grouped = pending.group_by { |d| [d.resource_type, d.resource_id] }

      grouped.each do |(resource_type, resource_id), deps|
        deps.each(&:destroy!)

        remaining = Etlify::SyncDependency.where(
          crm_name: crm_name.to_s,
          resource_type: resource_type,
          resource_id: resource_id
        )

        next if remaining.exists?

        enqueue_sync(resource_type, resource_id, crm_name)
      end
    end

    # Clean up dependency records for a child resource.
    #
    # @param resource [ActiveRecord::Base]
    # @param crm_name [Symbol]
    def self.cleanup_for_child!(resource, crm_name:)
      return unless table_exists?

      Etlify::SyncDependency.pending_for_child(
        resource,
        crm_name: crm_name
      ).delete_all
    end

    def self.table_exists?
      return @table_exists if defined?(@table_exists)

      @table_exists = ActiveRecord::Base.connection.data_source_exists?(
        "etlify_sync_dependencies"
      )
    end

    def self.reset_table_exists_cache!
      remove_instance_variable(:@table_exists) if defined?(@table_exists)
    end

    def self.ensure_table_exists!
      return if table_exists?

      raise Etlify::MissingTableError,
            'Table "etlify_sync_dependencies" does not exist. ' \
            "Run: rails g etlify:sync_dependencies_migration && " \
            "rails db:migrate"
    end
    private_class_method :ensure_table_exists!

    def self.enqueue_sync(resource_type, resource_id, crm_name)
      model_class = resource_type.constantize
      conf = model_class.etlify_crms[crm_name.to_sym]
      job_class = resolve_job_class(conf)

      if job_class.respond_to?(:perform_later)
        job_class.perform_later(resource_type, resource_id, crm_name.to_s)
      elsif job_class.respond_to?(:perform_async)
        job_class.perform_async(resource_type, resource_id, crm_name.to_s)
      end
    end
    private_class_method :enqueue_sync

    def self.resolve_job_class(conf)
      given = conf&.dig(:job_class)
      return Etlify::SyncJob unless given

      given.is_a?(String) ? given.constantize : given
    end
    private_class_method :resolve_job_class
  end
end
