module Etlify
  class EventTracker
    attr_accessor(
      :adapter,
      :conf,
      :contact_properties,
      :crm_name,
      :name,
      :occurred_at,
      :properties,
      :resource,
      :sync_first
    )

    def self.call(
      resource,
      crm_name:,
      name:,
      properties: {},
      contact_properties: {},
      occurred_at: nil,
      sync_first: true
    )
      new(
        resource,
        crm_name: crm_name,
        name: name,
        properties: properties,
        contact_properties: contact_properties,
        occurred_at: occurred_at,
        sync_first: sync_first
      ).call
    end

    def initialize(
      resource,
      crm_name:,
      name:,
      properties: {},
      contact_properties: {},
      occurred_at: nil,
      sync_first: true
    )
      @resource           = resource
      @crm_name           = crm_name.to_sym
      @conf               = resource.class.etlify_crms.fetch(@crm_name)
      @adapter            = @conf[:adapter]
      @name               = name
      @properties         = properties || {}
      @contact_properties = contact_properties || {}
      @occurred_at        = occurred_at
      @sync_first         = sync_first

      unless @adapter.respond_to?(:track_event!)
        raise ArgumentError, "Adapter must respond to track_event!"
      end
    end

    def call
      return :disabled unless Etlify::CRM.enabled?(crm_name)
      return track!(*match_by_identifier) unless sync_first

      result = Etlify::Synchronizer.call(resource, crm_name: crm_name)
      return :skipped if result == :skipped

      if result == :buffered
        raise Etlify::SyncError,
              "Cannot track event #{name}: sync buffered until its dependencies are synced"
      end

      crm_id = sync_line&.crm_id
      return :sync_failed unless result.in?([:synced, :not_modified]) && crm_id.present?

      track!("crm_id", crm_id)
    end

    private

    def track!(identifier_property, identifier_value)
      adapter.track_event!(
        name: name,
        identifier_property: identifier_property,
        identifier_value: identifier_value,
        properties: properties,
        contact_properties: contact_properties,
        occurred_at: occurred_at
      )
      :tracked
    end

    def match_by_identifier
      [
        Etlify::MatchBy.property(conf),
        Etlify::MatchBy.resolve(resource, conf),
      ]
    end

    def sync_line
      resource.crm_synchronisations.find_by(crm_name: crm_name)
    end
  end
end
