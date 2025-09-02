require "json"
require "uri"
require "net/http"

module Etlify
  module Adapters
    # Airtable REST adapter (public HTTP API, endpoint namespace v0).
    #
    # This mirrors HubspotV3Adapter's public surface so it can be dropped in
    # as-is by Etlify. It purposely keeps zero runtime deps and allows DI of
    # an HTTP client for testing.
    #
    # Notes
    # - Upsert strategy: optional lookup on `id_property` using filterByFormula
    #   then `PATCH` on hit, else `POST` to create.
    # - Delete: returns true on 2xx, false on 404, raises otherwise.
    # - Errors: maps common HTTP statuses to Etlify exceptions.
    # - Transport errors: wrapped into Etlify::TransportError.
    class AirtableV0Adapter
      API_BASE = "https://api.airtable.com/v0"

      # @param api_key [String] Airtable personal access token
      # @param base_id [String] Airtable Base ID (e.g., "app...")
      # @param table [String, nil] Optional default table name
      # @param http_client [#request] Optional injected HTTP client
      def initialize(api_key:, base_id:, table: nil, http_client: nil)
        @api_key = api_key
        @base_id = base_id
        @default_table = table
        @http = http_client || DefaultHttp.new
      end

      # Upsert a record into `object_type` (table). If `id_property` is given
      # and present in payload, we try to find the record and PATCH it. If not
      # found (or no id_property), we POST to create.
      #
      # @return [String, nil] Airtable record id (e.g., "rec...") or nil
      def upsert!(payload:, object_type: nil, id_property: nil)
        table = resolve_table!(object_type)
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        fields = payload.dup
        unique_value = nil

        if id_property
          key_str = id_property.to_s
          key_sym = key_str.to_sym
          unique_value = fields.delete(key_str) || fields.delete(key_sym)
        end

        record_id = if id_property && unique_value
          find_record_id_by_field(table, id_property, unique_value)
        end

        if record_id
          update_record(table, record_id, fields)
          record_id.to_s
        else
          create_record(table, fields, id_property, unique_value)
        end
      end

      # Delete a record by Airtable record id (rec...).
      # @return [Boolean] true on 2xx, false on 404
      def delete!(crm_id:, object_type: nil)
        table = resolve_table!(object_type)
        raise ArgumentError, "crm_id must be provided" if crm_id.to_s.empty?

        path = "/#{@base_id}/#{enc(table)}/#{crm_id}"
        resp = request(:delete, path)
        return true if resp[:status].between?(200, 299)
        return false if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      private

      # Simple Net::HTTP client for dependency-free default.
      class DefaultHttp
        def request(method, url, headers: {}, body: nil)
          uri = URI(url)
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = uri.scheme == "https"

          klass = {
            get: Net::HTTP::Get,
            post: Net::HTTP::Post,
            patch: Net::HTTP::Patch,
            delete: Net::HTTP::Delete,
          }.fetch(method) { raise ArgumentError, "Unsupported method: #{method}" }

          req = klass.new(uri.request_uri, headers)
          req.body = body if body

          res = http.request(req)
          {status: res.code.to_i, body: res.body}
        end
      end

      def resolve_table!(object_type)
        table = object_type || @default_table
        if !table || table.to_s.empty?
          raise ArgumentError, "object_type (table) must be provided"
        end

        table
      end

      def request(method, path, body: nil, query: {})
        url = API_BASE + path
        url += "?#{URI.encode_www_form(query)}" unless query.empty?

        headers = {
          "Authorization" => "Bearer #{@api_key}",
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
        err = payload["error"] if payload

        message = if err.is_a?(Hash)
          err["message"] || "Airtable API request failed"
        else
          payload["message"] || "Airtable API request failed"
        end

        type = err["type"] if err.is_a?(Hash)
        full = "#{message} (status=#{status}, path=#{path}"
        full << ", type=#{type}" if type
        full << ")"

        klass = case status
        when 401, 403 then Etlify::Unauthorized
        when 404 then Etlify::NotFound
        when 409, 422 then Etlify::ValidationFailed
        when 429 then Etlify::RateLimited
        else Etlify::ApiError
        end

        raise klass.new(
          full,
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

      def find_record_id_by_field(table, field, value)
        formula = build_equality_formula(field.to_s, value)
        path = "/#{@base_id}/#{enc(table)}"
        query = {filterByFormula: formula, maxRecords: 1, pageSize: 1}

        resp = request(:get, path, query: query)

        if resp[:status] == 200 && resp[:json].is_a?(Hash)
          recs = resp[:json]["records"]
          return recs.first["id"] if recs.is_a?(Array) && recs.any?

          return nil
        end

        return nil if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      def update_record(table, record_id, fields)
        path = "/#{@base_id}/#{enc(table)}/#{record_id}"
        body = {fields: stringify_keys(fields)}
        resp = request(:patch, path, body: body)
        raise_for_error!(resp, path: path)
        true
      end

      def create_record(table, fields, id_property, unique_value)
        path = "/#{@base_id}/#{enc(table)}"
        fs = stringify_keys(fields)
        if id_property && unique_value && !fs.key?(id_property.to_s)
          fs[id_property.to_s] = unique_value
        end

        resp = request(:post, path, body: {fields: fs})

        if resp[:status].between?(200, 299) &&
            resp[:json].is_a?(Hash) &&
            resp[:json]["id"]
          return resp[:json]["id"].to_s
        elsif resp[:status].between?(200, 299)
          # 2xx sans id => cas inattendu => ApiError
          raise Etlify::ApiError.new(
            "Airtable create returned 2xx without id (status=#{resp[:status]})",
            status: resp[:status],
            code: nil,
            category: nil,
            correlation_id: nil,
            details: resp[:json],
            raw: resp[:body]
          )
        end

        raise_for_error!(resp, path: path)
      end

      def stringify_keys(hash)
        hash.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      end

      def build_equality_formula(field, value)
        lhs = "{" + field.to_s.gsub("}", ")") + "}"
        rhs = case value
        when String
          escaped = value.to_s.each_char.map { |ch| (ch == "'") ? "'" : ch }.join
          "'" + escaped + "'"
        when TrueClass, FalseClass
          value ? "TRUE()" : "FALSE()"
        when Numeric
          value.to_s
        else
          json = JSON.dump(value)
          escaped = json.each_char.map { |ch| (ch == "'") ? "'" : ch }.join
          "'" + escaped + "'"
        end
        "#{lhs} = #{rhs}"
      end

      def enc(str)
        URI.encode_www_form_component(str)
      end
    end
  end
end
