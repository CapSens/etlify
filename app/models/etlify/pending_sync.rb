module Etlify
  class PendingSync < ActiveRecord::Base
    self.table_name = "etlify_pending_syncs"

    belongs_to :dependent, polymorphic: true
    belongs_to :dependency, polymorphic: true

    validates :crm_name, presence: true
    validates :dependent_id, uniqueness: {
      scope: [:dependent_type, :dependency_type, :dependency_id, :crm_name]
    }

    scope :for_dependency, ->(resource, crm_name:) {
      where(
        dependency_type: resource.class.name,
        dependency_id: resource.id,
        crm_name: crm_name.to_s
      )
    }
  end
end
