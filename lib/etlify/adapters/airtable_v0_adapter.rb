require_relative "default_http"
require_relative "airtable_v0/client"
require_relative "airtable_v0/formula"

module Etlify
  module Adapters
    # Airtable Adapter (API v0) with per-call table type.
    #
    # Matching contract: the payload is synced as-is. `match_property` /
    # `match_value` are used to find the record; the matching field is only
    # written when the payload does not include it AND the operation needs
    # it (creation, or performUpsert which requires the merge field in
    # `fields`). Writing it on a matched record is a no-op since Airtable
    # matches on strict field equality.
    #
    # Usage:
    #   adapter = Etlify::Adapters::AirtableV0Adapter.new(
    #     access_token: ENV["AIRTABLE_TOKEN"],
    #     base_id: "appXXXXXXXXXXXXXX",
    #   )
    #   adapter.upsert!(
    #     object_type: "tblXXX",
    #     payload: {Name: "John"},
    #     match_property: "Email",
    #     match_value: "john@example.com"
    #   )
    #   adapter.delete!(object_type: "tblXXX", crm_id: "recXXX")
    #   adapter.batch_upsert!(
    #     object_type: "tblXXX",
    #     inputs: [{value: "john@example.com", properties: {Name: "John"}}],
    #     match_property: "Email"
    #   )
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

      # For bulk operations, prefer batch_upsert! which uses
      # Airtable's native performUpsert (up to 10 rec/req).
      def upsert!(object_type:, payload:, match_property:, match_value:, crm_id: nil)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        validate_present!(:match_property, match_property)

        prop = match_property.to_s
        value = match_value.to_s.strip

        object_id = if crm_id.to_s.strip.empty?
          if value.empty?
            raise ArgumentError,
                  "match_value must be provided when crm_id is unknown"
          end

          find_record_by_field(object_type, prop, value)
        else
          crm_id.to_s.strip
        end

        if object_id
          update_record(object_type, object_id, payload)
          object_id.to_s
        else
          create_record(object_type, creation_fields(payload, prop, value))
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
      #
      # Unlike HubSpot, Airtable's performUpsert has no separate id slot:
      # the merge value is read from `fields`. The match field is therefore
      # injected into each record's fields when the payload does not carry
      # it. This is safe: matching is a strict equality on that field, so a
      # matched record already holds the exact injected value (no-op write).
      # @param inputs [Array<Hash>] each {value:, properties:} where value is
      #   the match_property value and properties the payload
      # @param match_property [String] merge field name or field ID
      # @return [Hash{String => String}] mapping of each input's value (as
      #   provided) to Airtable record ID
      def batch_upsert!(object_type:, inputs:, match_property:)
        validate_string!(:object_type, object_type)
        validate_present!(:match_property, match_property)
        if !inputs.is_a?(Array) || inputs.empty?
          raise ArgumentError,
                "inputs must be a non-empty Array"
        end

        path = @client.base_path(object_type)
        prop_key = match_property.to_s

        # Airtable returns response fields keyed by NAME by default. When the
        # caller uses field IDs (e.g. "fldXXXXXXXXXXXXXX") for match_property,
        # we must request the response with field IDs too, otherwise
        # extract_batch_mapping cannot find the match_property value back and
        # returns an empty mapping (silently writing crm_id: nil).
        use_field_ids = AIRTABLE_FIELD_ID_REGEX.match?(prop_key)

        inputs.each_slice(BATCH_MAX_SIZE).each_with_object({}) do |slice, mapping|
          values = slice.map do |input|
            raw = (input[:value] || input["value"]).to_s
            value = raw.strip
            if value.empty?
              raise ArgumentError,
                    "every input must carry a non-blank :value"
            end

            [raw, value]
          end

          body = {
            performUpsert: {
              fieldsToMergeOn: [prop_key],
            },
            records: slice.each_with_index.map do |input, index|
              fields = input[:properties] || input["properties"] || {}
              {fields: creation_fields(fields, prop_key, values[index].last)}
            end,
          }
          body[:returnFieldsByFieldId] = true if use_field_ids

          response = @client.patch(path, body: body)
          @client.raise_for_error!(response, path: path)

          by_stored_value = extract_batch_mapping(response, prop_key)
          values.each do |raw, value|
            record_id = by_stored_value[value]
            mapping[raw] = record_id if record_id
          end
        end
      end

      # Batch update targeting each record by its known Airtable record ID
      # (crm_id) instead of match_property. Uses Airtable's PATCH with
      # explicit record ids (up to 10 records per request).
      # @param object_type [String] Airtable table id/name
      # @param records [Array<Hash>] each {crm_id:, properties:}
      # @return [Hash{String => String}] identity mapping {crm_id => crm_id}
      def batch_update!(object_type:, records:)
        validate_string!(:object_type, object_type)
        if !records.is_a?(Array) || records.empty?
          raise ArgumentError,
                "records must be a non-empty Array"
        end

        path = @client.base_path(object_type)

        records.each_slice(BATCH_MAX_SIZE).each_with_object({}) do |slice, mapping|
          body = {
            records: slice.map do |record|
              {
                id: fetch_crm_id(record),
                fields: stringify_keys(record[:properties] || record["properties"] || {}),
              }
            end,
          }

          response = @client.patch(path, body: body)
          @client.raise_for_error!(response, path: path)

          slice.each do |record|
            id = fetch_crm_id(record)
            mapping[id] = id unless id.empty?
          end
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

      # Fields written when the record may not exist yet: the match field is
      # injected only when the payload does not already carry it (payload
      # wins, whether keyed by string or symbol).
      def creation_fields(payload, match_property, match_value)
        fields = stringify_keys(payload)
        fields[match_property] = match_value unless fields.key?(match_property)
        fields
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

      def extract_batch_mapping(response, match_property)
        records = extract_records(response)
        records.each_with_object({}) do |r, h|
          record_id = r["id"].to_s
          fields = r["fields"] || {}
          match_value = (fields[match_property] || "").to_s.strip
          h[match_value] = record_id unless match_value.empty?
        end
      end

      def stringify_keys(hash)
        hash.transform_keys(&:to_s)
      end

      def fetch_crm_id(record)
        (record[:crm_id] || record["crm_id"]).to_s
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
