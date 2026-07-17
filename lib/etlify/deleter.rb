module Etlify
  class Deleter
    attr_accessor(
      :adapter,
      :conf,
      :crm_id,
      :crm_name,
      :resource
    )

    # @param resource [ActiveRecord::Base]
    # @param crm_name [Symbol,String]
    # @param crm_id [String,nil] explicit CRM id, bypasses the sync line lookup.
    #   Use it to delete a remote row after the resource (and its sync line)
    #   has already been destroyed locally.
    def self.call(resource, crm_name:, crm_id: nil)
      new(resource, crm_name: crm_name, crm_id: crm_id).call
    end

    def initialize(resource, crm_name:, crm_id: nil)
      @resource = resource
      @crm_name = crm_name.to_sym
      @crm_id  = crm_id
      @conf    = resource.class.etlify_crms.fetch(@crm_name)
      @adapter = @conf[:adapter]
    end

    def call
      return :disabled unless Etlify::CRM.enabled?(crm_name)

      id = crm_id.presence || sync_line&.crm_id
      return :noop unless id.present?

      @adapter.delete!(
        crm_id: id,
        object_type: conf[:crm_object_type]
      )
      :deleted
    rescue => e
      error = Etlify::SyncError.new(e.message)
      error.set_backtrace(e.backtrace)
      raise error
    end

    private

    def sync_line
      resource.crm_synchronisations.find_by(crm_name: crm_name)
    end
  end
end
