require_relative "default_http"
require_relative "airtable_v0/client"
require_relative "airtable_v0/formula"

module Etlify
  module Adapters
    # Airtable Adapter (API v0) with per-call table type.
    #
    # Usage:
    #   adapter = Etlify::Adapters::AirtableV0Adapter.new(
    #     access_token: ENV["AIRTABLE_TOKEN"],
    #     base_id: "appXXXXXXXXXXXXXX",
    #   )
    #   adapter.upsert!(object_type: "tblXXX", payload: {Name: "John"}, id_property: "Email")
    #   adapter.delete!(object_type: "tblXXX", crm_id: "recXXX")
    #   adapter.batch_upsert!(object_type: "tblXXX", records: [...], id_property: "Email")
    #   adapter.batch_delete!(object_type: "tblXXX", crm_ids: ["recAAA", "recBBB"])
    class AirtableV0Adapter
      BATCH_MAX_SIZE = 10
      AIRTABLE_FIELD_ID_REGEX = /\Afld[A-Za-z0-9]{14}\z/.freeze

      def rate_limiter
        @client.rate_limiter
      end

      def rate_limiter=(limiter)
        @client.rate_limiter = limiter
      end

      def initialize(access_token:, base_id:, http_client: nil)
        validate_string!(:access_token, access_token)
        validate_string!(:base_id, base_id)

        @client = AirtableV0::Client.new(
          access_token: access_token,
          base_id: base_id,
          http: http_client || DefaultHttp.new
        )
      end

      # --- Standard Etlify interface ---

      # Note: unlike HubSpot, id_property is kept in the
      # payload because Airtable requires the field in
      # `fields` for both create and update operations.
      # For bulk operations, prefer batch_upsert! which uses
      # Airtable's native performUpsert (up to 10 rec/req).
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        properties = payload.dup
        object_id  = resolve_object_id(
          object_type, properties, id_property, crm_id
        )

        if object_id
          update_record(object_type, object_id, properties)
          object_id.to_s
        else
          create_record(object_type, properties)
        end
      end

      def delete!(object_type:, crm_id:)
        validate_string!(:object_type, object_type)
        validate_string!(:crm_id, crm_id)

        path = @client.record_path(object_type, crm_id)
        response = @client.delete(path)

        return true if response[:status].between?(200, 299)
        return false if response[:status] == 404

        @client.raise_for_error!(response, path: path)
      end

      # --- Batch operations (Airtable-specific) ---

      # Note: if a later slice fails, records from earlier
      # slices are already committed. Callers should handle
      # partial success when processing large batches.
      # @return [Hash{String => String}] mapping of id_property value to Airtable record ID
      def batch_upsert!(object_type:, records:, id_property:)
        validate_string!(:object_type, object_type)
        validate_present!(:id_property, id_property)
        if !records.is_a?(Array) || records.empty?
          raise ArgumentError,
                "records must be a non-empty Array"
        end

        path = @client.base_path(object_type)
        prop_key = id_property.to_s

        # Airtable returns response fields keyed by NAME by default. When the
        # caller uses field IDs (e.g. "fldXXXXXXXXXXXXXX") for id_property,
        # we must request the response with field IDs too, otherwise
        # extract_batch_mapping cannot find the id_property value back and
        # returns an empty mapping (silently writing crm_id: nil).
        use_field_ids = AIRTABLE_FIELD_ID_REGEX.match?(prop_key)

        records.each_slice(BATCH_MAX_SIZE).each_with_object({}) do |slice, mapping|
          body = {
            performUpsert: {
              fieldsToMergeOn: [prop_key],
            },
            records: slice.map { |fields| {fields: stringify_keys(fields)} },
          }
          body[:returnFieldsByFieldId] = true if use_field_ids

          response = @client.patch(path, body: body)
          @client.raise_for_error!(response, path: path)
          extract_batch_mapping(response, prop_key).each { |k, v| mapping[k] = v }
        end
      end

      # Note: if a later slice fails, records from earlier
      # slices are already deleted. Callers should handle
      # partial success when processing large batches.
      # @return [Array<Hash>] Airtable record hashes (with deleted: true)
      def batch_delete!(object_type:, crm_ids:)
        validate_string!(:object_type, object_type)
        if !crm_ids.is_a?(Array) || crm_ids.empty?
          raise ArgumentError,
                "crm_ids must be a non-empty Array"
        end

        path = @client.base_path(object_type)

        crm_ids.each_slice(BATCH_MAX_SIZE).flat_map do |slice|
          query = slice.map { |id| ["records[]", id.to_s] }
          response = @client.delete(path, query: query)
          @client.raise_for_error!(response, path: path)
          extract_records(response)
        end
      end

      private

      # --- Record operations ---

      def find_record_by_field(object_type, field_name, value)
        formula = AirtableV0::Formula.eq(field_name, value)
        path = @client.base_path(object_type)

        response = @client.get(path, query: {
          "filterByFormula" => formula,
          "maxRecords" => 1,
        })

        return first_record_id(response) if response[:status] == 200
        return nil if response[:status] == 404

        @client.raise_for_error!(response, path: path)
      end

      def create_record(object_type, payload)
        path = @client.base_path(object_type)
        response = @client.post(path, body: {fields: stringify_keys(payload)})
        @client.raise_for_error!(response, path: path)

        record_id = response[:json].is_a?(Hash) && response[:json]["id"]

        unless record_id
          raise Etlify::ApiError.new(
            "Airtable create succeeded but returned no record id (path=#{path})",
            status: response[:status],
            raw: response[:body]
          )
        end

        record_id.to_s
      end

      def update_record(object_type, record_id, payload)
        path = @client.record_path(object_type, record_id)
        response = @client.patch(path, body: {fields: stringify_keys(payload)})
        @client.raise_for_error!(response, path: path)
        true
      end

      # --- Helpers ---

      def resolve_object_id(object_type, properties, id_property, crm_id)
        unless crm_id.to_s.strip.empty?
          return crm_id.to_s.strip
        end

        return nil unless id_property

        key_str = id_property.to_s
        unique_value = properties[key_str] || properties[key_str.to_sym]

        return nil unless unique_value

        find_record_by_field(object_type, key_str, unique_value)
      end

      def first_record_id(response)
        return nil unless response[:json].is_a?(Hash)

        records = response[:json]["records"]
        return nil unless records.is_a?(Array) && records.any?

        records.first["id"]
      end

      def extract_records(response)
        returned = response[:json].is_a?(Hash) ? response[:json]["records"] : nil
        returned.is_a?(Array) ? returned : []
      end

      def extract_batch_mapping(response, id_property)
        records = extract_records(response)
        records.each_with_object({}) do |r, h|
          record_id = r["id"].to_s
          fields = r["fields"] || {}
          id_value = (fields[id_property] || "").to_s
          h[id_value] = record_id unless id_value.empty?
        end
      end

      def stringify_keys(hash)
        hash.transform_keys(&:to_s)
      end

      def validate_string!(name, value)
        return if value.is_a?(String) && !value.empty?

        raise ArgumentError, "#{name} must be a non-empty String"
      end

      def validate_present!(name, value)
        return unless value.nil? || value.to_s.empty?

        raise ArgumentError, "#{name} must be provided"
      end
    end
  end
end
