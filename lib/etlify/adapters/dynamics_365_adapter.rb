require_relative "default_http"
require_relative "dynamics_365/client"
require_relative "dynamics_365/token_cache"

module Etlify
  module Adapters
    # Microsoft Dynamics 365 (Dataverse Web API v9.2) Adapter.
    #
    # Authentication is OAuth 2.0 client_credentials. The bearer
    # token is cached in-memory per process and refreshed
    # transparently before expiration (and on a 401 retry).
    #
    # Usage:
    #   adapter = Etlify::Adapters::Dynamics365Adapter.new(
    #     tenant_id: ENV["DYNAMICS_TENANT_ID"],
    #     client_id: ENV["DYNAMICS_CLIENT_ID"],
    #     client_secret: ENV["DYNAMICS_CLIENT_SECRET"],
    #     resource_uri: ENV["DYNAMICS_RESOURCE_URI"],
    #   )
    #   adapter.upsert!(
    #     object_type: "contacts",
    #     payload: {firstname: "John", emailaddress1: "j@example.com"},
    #     id_property: "emailaddress1",
    #   )
    #   adapter.delete!(object_type: "contacts", crm_id: guid)
    #
    # Limitations:
    # - batch_upsert! and batch_delete! raise NotImplementedError.
    #   Native Dataverse $batch (multipart/mixed) support is
    #   planned in a follow-up. Use Etlify::SyncJob (per-record)
    #   rather than Etlify::BatchSyncJob with this adapter for now.
    # - id_property must be configured as a Dataverse alternate key
    #   on the target entity, otherwise upsert! will return a 400.
    class Dynamics365Adapter
      DEFAULT_API_VERSION = "9.2"
      RESOURCE_URI_REGEX = %r{\Ahttps://}.freeze
      API_VERSION_REGEX = /\A\d+\.\d+\z/.freeze

      def rate_limiter
        @client.rate_limiter
      end

      def rate_limiter=(limiter)
        @client.rate_limiter = limiter
      end

      def initialize(
        tenant_id:,
        client_id:,
        client_secret:,
        resource_uri:,
        api_version: DEFAULT_API_VERSION,
        http_client: nil,
        token_cache: nil
      )
        validate_string!(:tenant_id, tenant_id)
        validate_string!(:client_id, client_id)
        validate_string!(:client_secret, client_secret)
        validate_string!(:resource_uri, resource_uri)
        validate_string!(:api_version, api_version)
        validate_resource_uri!(resource_uri)
        validate_api_version!(api_version)

        @client = Dynamics365::Client.new(
          tenant_id: tenant_id,
          client_id: client_id,
          client_secret: client_secret,
          resource_uri: resource_uri,
          api_version: api_version,
          http: http_client || DefaultHttp.new,
          token_cache: token_cache || Dynamics365::TokenCache.new
        )
      end

      # @return [String] Dataverse GUID of the upserted record
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        normalized_crm_id = crm_id.to_s.strip
        unless normalized_crm_id.empty?
          @client.patch_by_guid(object_type, normalized_crm_id, payload)
          return normalized_crm_id
        end

        if id_property
          key_value = lookup_payload_value(payload, id_property)
          unless key_value.nil? || key_value.to_s.empty?
            response = @client.patch_by_alternate_key(
              object_type, id_property.to_s, key_value, payload
            )
            return response[:crm_id]
          end
        end

        response = @client.post_create(object_type, payload)
        response[:crm_id]
      end

      def delete!(object_type:, crm_id:)
        validate_string!(:object_type, object_type)
        validate_string!(:crm_id, crm_id)

        response = @client.delete_by_guid(object_type, crm_id)
        return false if response[:status] == 404

        true
      end

      def batch_upsert!(object_type:, records:, id_property:)
        raise NotImplementedError, batch_not_supported_message
      end

      def batch_delete!(object_type:, crm_ids:)
        raise NotImplementedError, batch_not_supported_message
      end

      private

      def lookup_payload_value(payload, id_property)
        key_str = id_property.to_s
        payload[key_str] || payload[key_str.to_sym]
      end

      def batch_not_supported_message
        [
          "Batch operations are not yet supported by",
          "Dynamics365Adapter. Use upsert!/delete! per record,",
          "or wait for the multipart $batch implementation.",
        ].join(" ")
      end

      def validate_string!(name, value)
        return if value.is_a?(String) && !value.empty?

        raise ArgumentError, "#{name} must be a non-empty String"
      end

      def validate_resource_uri!(value)
        return if value.match?(RESOURCE_URI_REGEX)

        raise ArgumentError, "resource_uri must start with https://"
      end

      def validate_api_version!(value)
        return if value.match?(API_VERSION_REGEX)

        raise ArgumentError,
              "api_version must match \\A\\d+\\.\\d+\\z (e.g. \"9.2\")"
      end
    end
  end
end
