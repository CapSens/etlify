class CrmSynchronisation < ActiveRecord::Base
  self.table_name = "crm_synchronisations"

  belongs_to :resource, polymorphic: true

  validates :crm_id, allow_nil: true, uniqueness: {scope: [:crm_name, :resource_type]}
  validates :resource_type, presence: true
  validates :resource_id, presence: true
  validates :resource_id, uniqueness: {scope: [:resource_type, :crm_name]}
  validates :crm_name, presence: true, uniqueness: {scope: :resource}

  def stale?(digest)
    last_digest != digest
  end

  scope :with_error, -> { where.not(last_error: nil) }
  scope :without_error, -> { where(last_error: nil) }
end
