class CrmSynchronisation < ActiveRecord::Base
  self.table_name = "crm_synchronisations"

  belongs_to :resource, polymorphic: true

  validates :crm_id, uniqueness: true, allow_nil: true
  validates :resource_type, presence: true
  validates :resource_id, presence: true
  validates :resource_id, uniqueness: {scope: [:resource_type, :crm_name]}
  validates :crm_name, presence: true, uniqueness: {scope: :resource}

  def stale?(digest)
    last_digest != digest
  end

  scope :with_error, -> { where.not(last_error: nil) }
  scope :without_error, -> { where(last_error: nil) }

  scope :retry_exhausted, ->(limit) {
    where(arel_table[:error_count].gteq(limit))
  }

  scope :retryable, ->(limit) {
    where(
      arel_table[:error_count].lt(limit)
        .or(arel_table[:error_count].eq(nil))
    )
  }

  def reset_error_count!
    update!(error_count: 0, last_error: nil)
  end
end
