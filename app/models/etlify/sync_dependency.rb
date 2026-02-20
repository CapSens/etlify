# frozen_string_literal: true

module Etlify
  class SyncDependency < ActiveRecord::Base
    self.table_name = "etlify_sync_dependencies"

    belongs_to :resource, polymorphic: true
    belongs_to :parent_resource, polymorphic: true

    validates :crm_name, presence: true
    validates :resource_type, presence: true
    validates :resource_id, presence: true
    validates :parent_resource_type, presence: true
    validates :parent_resource_id, presence: true
    validates :resource_id,
              uniqueness: {
                scope: [
                  :crm_name,
                  :resource_type,
                  :parent_resource_type,
                  :parent_resource_id,
                ],
              }

    scope :for_crm, ->(crm_name) { where(crm_name: crm_name.to_s) }

    scope :pending_for_parent, lambda { |parent, crm_name:|
      where(
        crm_name: crm_name.to_s,
        parent_resource_type: parent.class.name,
        parent_resource_id: parent.id
      )
    }

    scope :pending_for_child, lambda { |child, crm_name:|
      where(
        crm_name: crm_name.to_s,
        resource_type: child.class.name,
        resource_id: child.id
      )
    }
  end
end
