# frozen_string_literal: true

require "json"
require "uri"
require "net/http"

module Etlify
  module Adapters
    # Airtable Adapter (API v0) with per-call table type.
    #
    # Error handling:
    # - Non-2xx responses raise specific exceptions (Unauthorized, NotFound, RateLimited, ValidationFailed, ApiError).
    # - Transport-level issues raise TransportError.
    # - delete! returns false on 404 (record not found), raises otherwise.
    #
    # Batch support:
    # - batch_upsert! uses Airtable's native performUpsert (up to 10 records per request).
    # - batch_delete! removes up to 10 records per request.
    #
    # Usage:
    #   adapter = Etlify::Adapters::AirtableV0Adapter.new(
    #     access_token: ENV["AIRTABLE_TOKEN"],
    #     base_id: "appXXXXXXXXXXXXXX",
    #   )
    #   adapter.upsert!(object_type: "tblXXX", payload: {Name: "John"}, id_property: "Email", crm_id: nil)
    #   adapter.delete!(object_type: "tblXXX", crm_id: "recXXX") # => true, or false if 404
    #   adapter.batch_upsert!(object_type: "tblXXX", records: [{Name: "A"}, {Name: "B"}], id_property: "Email")
    #   adapter.batch_delete!(object_type: "tblXXX", crm_ids: ["recAAA", "recBBB"])
    class AirtableV0Adapter
      API_BASE = "https://api.airtable.com/v0"
      BATCH_MAX_SIZE = 10

      # @param access_token [String] Airtable personal access token or API key
      # @param base_id [String] Airtable base ID (e.g., "appXXXXXXXXXXXXXX")
      # @param http_client [#request] Optional HTTP client for tests. Signature: request(method, url, headers:, body:)
      def initialize(access_token:, base_id:, http_client: nil)
        unless access_token.is_a?(String) && !access_token.empty?
          raise ArgumentError,
                "access_token must be a non-empty String"
        end
        raise ArgumentError, "base_id must be a non-empty String" unless base_id.is_a?(String) && !base_id.empty?

        @access_token = access_token
        @base_id      = base_id
        @http         = http_client || DefaultHttp.new
      end

      # Upsert by searching on id_property (if provided), otherwise create directly.
      # @param object_type [String] Airtable table ID or name
      # @param payload [Hash] Fields for the record
      # @param id_property [String, nil] Field name used to search for existing record
      # @param crm_id [String, nil] Airtable record ID if known (e.g., "recXXX")
      # @return [String] Airtable record ID
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        properties   = payload.dup
        unique_value = nil

        if crm_id.to_s.strip.empty?
          if id_property
            key_str = id_property.to_s
            key_sym = key_str.to_sym
            unique_value =
              properties[key_str] || properties[key_sym]
          end

          object_id = (find_record_by_field(object_type, id_property.to_s, unique_value) if id_property && unique_value)
        else
          object_id = crm_id.to_s.strip
        end

        if object_id
          update_record(object_type, object_id, properties)
          object_id.to_s
        else
          create_record(object_type, properties)
        end
      end

      # Delete a record by its Airtable record ID.
      # @param object_type [String] Table ID or name
      # @param crm_id [String] Airtable record ID (e.g., "recXXX")
      # @return [Boolean] true on 2xx, false on 404
      def delete!(object_type:, crm_id:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "crm_id must be provided" if crm_id.nil? || crm_id.to_s.empty?

        path = "/#{@base_id}/#{object_type}/#{crm_id}"
        resp = request(:delete, path)

        return true if resp[:status].between?(200, 299)
        return false if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      # Batch upsert using Airtable's native performUpsert.
      # @param object_type [String] Table ID or name
      # @param records [Array<Hash>] Array of field hashes
      # @param id_property [String] Field name to merge on
      # @return [Array<Hash>] Array of record hashes returned by Airtable
      def batch_upsert!(object_type:, records:, id_property:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "records must be a non-empty Array" unless records.is_a?(Array) && !records.empty?
        raise ArgumentError, "id_property must be provided" if id_property.nil? || id_property.to_s.empty?

        path = "/#{@base_id}/#{object_type}"
        all_results = []

        records.each_slice(BATCH_MAX_SIZE) do |slice|
          body = {
            performUpsert: {
              fieldsToMergeOn: [id_property.to_s],
            },
            records: slice.map { |fields| {fields: stringify_keys(fields)} },
          }

          resp = request(:patch, path, body: body)
          raise_for_error!(resp, path: path)

          returned = resp[:json].is_a?(Hash) ? resp[:json]["records"] : nil
          all_results.concat(returned) if returned.is_a?(Array)
        end

        all_results
      end

      # Batch delete records by their Airtable record IDs.
      # @param object_type [String] Table ID or name
      # @param crm_ids [Array<String>] Array of record IDs
      # @return [Array<Hash>] Array of {id:, deleted:} hashes
      def batch_delete!(object_type:, crm_ids:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "crm_ids must be a non-empty Array" unless crm_ids.is_a?(Array) && !crm_ids.empty?

        path = "/#{@base_id}/#{object_type}"
        all_results = []

        crm_ids.each_slice(BATCH_MAX_SIZE) do |slice|
          query = slice.map { |id| ["records[]", id.to_s] }
          resp = request(:delete, path, query: query)
          raise_for_error!(resp, path: path)

          returned = resp[:json].is_a?(Hash) ? resp[:json]["records"] : nil
          all_results.concat(returned) if returned.is_a?(Array)
        end

        all_results
      end

      private

      # Simple Net::HTTP client used by default (dependency-free)
      class DefaultHttp
        def request(method, url, headers: {}, body: nil)
          uri  = URI(url)
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = uri.scheme == "https"

          klass = {
            get: Net::HTTP::Get,
            post: Net::HTTP::Post,
            patch: Net::HTTP::Patch,
            delete: Net::HTTP::Delete,
          }.fetch(method) { raise ArgumentError, "Unsupported method: #{method.inspect}" }

          req = klass.new(uri.request_uri, headers)
          req.body = body if body

          res = http.request(req)
          {status: res.code.to_i, body: res.body}
        end
      end

      def request(method, path, body: nil, query: {})
        url = API_BASE + path
        if query.is_a?(Array)
          url += "?#{URI.encode_www_form(query)}" unless query.empty?
        elsif query.is_a?(Hash)
          url += "?#{URI.encode_www_form(query)}" unless query.empty?
        end

        headers = {
          "Authorization" => "Bearer #{@access_token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
        }

        raw_body = body && JSON.dump(body)

        begin
          res = @http.request(method, url, headers: headers, body: raw_body)
        rescue => e
          raise Etlify::TransportError.new(
            "HTTP transport error: #{e.class}: #{e.message}",
            status: 0,
            raw: nil
          )
        end

        res[:json] = parse_json_safe(res[:body])
        res
      end

      def raise_for_error!(resp, path:)
        status = resp[:status].to_i
        return if status.between?(200, 299)

        payload = resp[:json].is_a?(Hash) ? resp[:json] : {}
        err = payload["error"]

        message = if err.is_a?(Hash)
          err["message"] || "Airtable API request failed"
        else
          payload["message"] || "Airtable API request failed"
        end

        type = err["type"] if err.is_a?(Hash)
        full_message = "#{message} (status=#{status}, path=#{path}"
        full_message << ", type=#{type}" if type
        full_message << ")"

        klass =
          case status
          when 401, 403 then Etlify::Unauthorized
          when 404      then Etlify::NotFound
          when 409, 422 then Etlify::ValidationFailed
          when 429      then Etlify::RateLimited
          else Etlify::ApiError
          end

        raise klass.new(
          full_message,
          status: status,
          code: type,
          category: type,
          correlation_id: nil,
          details: err,
          raw: resp[:body]
        )
      end

      def parse_json_safe(str)
        return nil if str.nil? || str.empty?

        JSON.parse(str)
      rescue JSON::ParserError
        nil
      end

      def find_record_by_field(object_type, field_name, value)
        escaped = escape_formula_value(value)
        formula = "{#{field_name}} = #{escaped}"
        path = "/#{@base_id}/#{object_type}"

        resp = request(:get, path, query: {
          "filterByFormula" => formula,
          "maxRecords" => 1,
        })

        if resp[:status] == 200 && resp[:json].is_a?(Hash)
          records = resp[:json]["records"]
          return records.first["id"] if records.is_a?(Array) && records.any?

          return nil
        end

        return nil if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      def create_record(object_type, payload)
        path = "/#{@base_id}/#{object_type}"
        body = {fields: stringify_keys(payload)}
        resp = request(:post, path, body: body)

        if resp[:status].between?(200, 299) && resp[:json].is_a?(Hash) && resp[:json]["id"]
          return resp[:json]["id"].to_s
        end

        raise_for_error!(resp, path: path)
      end

      def update_record(object_type, record_id, payload)
        path = "/#{@base_id}/#{object_type}/#{record_id}"
        body = {fields: stringify_keys(payload)}
        resp = request(:patch, path, body: body)
        raise_for_error!(resp, path: path)
        true
      end

      def stringify_keys(hash)
        hash.transform_keys(&:to_s)
      end

      def escape_formula_value(value)
        if value.is_a?(Numeric)
          value.to_s
        else
          "\"#{value.to_s.gsub('\\', '\\\\\\\\').gsub('"', '\\"')}\""
        end
      end
    end
  end
end
