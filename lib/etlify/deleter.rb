module Etlify
  class Deleter
    attr_accessor(
      :adapter,
      :conf,
      :crm_name,
      :resource
    )

    # @param resource [ActiveRecord::Base]
    # @param crm [Symbol,String]
    def self.call(resource, crm_name:)
      new(resource, crm_name: crm_name).call
    end

    def initialize(resource, crm_name:)
      @resource = resource
      @crm_name = crm_name.to_sym
      @conf    = resource.class.etlify_crms.fetch(@crm_name)
      @adapter = @conf[:adapter].new
    end

    def call
      line = sync_line
      return :noop unless line&.crm_id.present?

      @adapter.delete!(
        crm_id: line.crm_id,
        object_type: conf[:crm_object_type],
        id_property: conf[:id_property]
      )
      :deleted
    rescue => e
      raise Etlify::SyncError, e.message
    end

    private

    def sync_line
      resource.crm_synchronisations.find_by(crm_name: crm_name)
    end
  end
end
