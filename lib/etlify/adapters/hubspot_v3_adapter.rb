require "json"
require "uri"
require "net/http"
require_relative "default_http"

module Etlify
  module Adapters
    # HubSpot Adapter (API v3) with per-call object type.
    # It supports native objects (e.g., "contacts", "companies", "deals") and custom objects (e.g., "p12345_myobject").
    #
    # Error handling:
    # - Non-2xx responses raise specific exceptions (Unauthorized, NotFound, RateLimited, ValidationFailed, ApiError).
    # - Transport-level issues raise TransportError.
    # - delete! returns false on 404 (object not found), raises otherwise.
    #
    # Matching contract: the payload is synced as-is. `match_property` /
    # `match_value` are only used to find the object (or to set the property
    # at creation time when the payload does not include it). The matching
    # property is NEVER written on an existing object unless the payload
    # explicitly includes it. For contacts matched by email, this prevents
    # overwriting the primary email when the platform email is one of the
    # contact's secondary emails (hs_additional_emails).
    #
    # Usage:
    #   adapter = Etlify::Adapters::HubspotV3Adapter.new(access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"])
    #   adapter.upsert!(
    #     object_type: "contacts",
    #     payload: {firstname: "John"},
    #     match_property: "email",
    #     match_value: "john@example.com",
    #     crm_id: nil
    #   )
    #   adapter.delete!(object_type: "contacts", crm_id: "123") # => true, or false if 404
    class HubspotV3Adapter
      API_BASE = "https://api.hubapi.com"
      BATCH_MAX_SIZE = 100

      attr_accessor :rate_limiter

      # @param access_token [String] HubSpot private app token
      # @param http_client [#request] Optional HTTP client for tests. Signature: request(method, url, headers:, body:)
      def initialize(access_token:, http_client: nil)
        @access_token = access_token
        @http         = http_client || Etlify::Adapters::DefaultHttp.new
      end

      # Upsert by crm_id when known, otherwise search on match_property /
      # match_value, otherwise create.
      # @param object_type [String] HubSpot CRM object type (e.g., "contacts", "companies", "deals", or a custom object)
      # @param payload [Hash] Properties for the object, synced as-is
      # @param match_property [String, Symbol] Unique property used to search
      #   (e.g., "email" for contacts, "domain" for companies)
      # @param match_value [String] Value of match_property for this record
      # @param crm_id [Integer, String, nil] Record's HubSpot hs_object_id if
      #   known (skips the match_property search entirely)
      # @return [String, nil] HubSpot hs_object_id as string or nil if not available
      def upsert!(object_type:, payload:, match_property:, match_value:, crm_id: nil)
        raise ArgumentError, "object_type must be a String" if !object_type.is_a?(String) || object_type.empty?
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)
        raise ArgumentError, "match_property must be provided" if match_property.to_s.strip.empty?

        prop = match_property.to_s
        value = normalize_match_value(prop, match_value)

        object_id = if crm_id.to_s.strip.empty?
          if value.empty?
            raise ArgumentError,
                  "match_value must be provided when crm_id is unknown"
          end

          find_object_id_by_property(object_type, prop, value)
        else
          crm_id.to_s.strip
        end

        if object_id
          update_object(object_type, object_id, payload)
          object_id.to_s
        else
          create_object(object_type, payload, prop, value)
        end
      end

      # Delete an object by hs_object_id.
      # @param object_type [String]
      # @param crm_id [String]
      # @return [Boolean] true on 2xx response, false on 404
      def delete!(object_type:, crm_id:)
        raise ArgumentError, "object_type must be a String" if !object_type.is_a?(String) || object_type.empty?
        raise ArgumentError, "crm_id must be provided" if crm_id.to_s.blank?

        path = "/crm/v3/objects/#{object_type}/#{crm_id}"
        resp = request(:delete, path)

        return true if resp[:status].between?(200, 299)
        return false if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      # Batch upsert via HubSpot's native /batch/upsert endpoint.
      # The matching value travels in the input's `id`/`idProperty` slots,
      # never in `properties`: HubSpot matches existing objects (including
      # contacts' secondary emails) without rewriting the matching property,
      # and sets it from `id` at creation time.
      # @param object_type [String] CRM object type
      # @param inputs [Array<Hash>] each {value:, properties:} where value is
      #   the match_property value and properties the payload, synced as-is
      # @param match_property [String] Unique property for matching (e.g., "email")
      # @return [Hash{String => String}] mapping of each input's value (as
      #   provided) to hs_object_id, so callers can look results up with the
      #   exact values they passed (HubSpot lowercases emails in responses)
      def batch_upsert!(object_type:, inputs:, match_property:)
        raise ArgumentError, "object_type must be a String" if !object_type.is_a?(String) || object_type.empty?
        raise ArgumentError, "match_property must be provided" if match_property.to_s.blank?
        raise ArgumentError, "inputs must be a non-empty Array" if !inputs.is_a?(Array) || inputs.empty?

        path = "/crm/v3/objects/#{object_type}/batch/upsert"
        prop = match_property.to_s

        inputs.each_slice(BATCH_MAX_SIZE).each_with_object({}) do |slice, mapping|
          values = slice.map do |input|
            raw = fetch_input(input, :value).to_s
            value = normalize_match_value(prop, raw)
            if value.empty?
              raise ArgumentError,
                    "every input must carry a non-blank :value"
            end

            [raw, value]
          end

          body = {
            inputs: slice.each_with_index.map do |input, index|
              {
                id: values[index].last,
                idProperty: prop,
                properties: stringify_keys(fetch_input(input, :properties) || {}),
              }
            end,
          }

          resp = request(:post, path, body: body)
          raise_for_error!(resp, path: path)

          by_normalized_value = extract_batch_mapping(resp, prop)
          values.each do |raw, value|
            crm_id = by_normalized_value[value]
            mapping[raw] = crm_id if crm_id
          end
        end
      end

      # Batch update via HubSpot's native /batch/update endpoint, targeting
      # each object by its known hs_object_id (crm_id) instead of
      # match_property.
      # @param object_type [String] CRM object type
      # @param records [Array<Hash>] each {crm_id:, properties:}
      # @return [Hash{String => String}] identity mapping {crm_id => crm_id}
      #   for successfully submitted records
      def batch_update!(object_type:, records:)
        raise ArgumentError, "object_type must be a String" if !object_type.is_a?(String) || object_type.empty?
        raise ArgumentError, "records must be a non-empty Array" if !records.is_a?(Array) || records.empty?

        path = "/crm/v3/objects/#{object_type}/batch/update"

        records.each_slice(BATCH_MAX_SIZE).each_with_object({}) do |slice, mapping|
          body = {
            inputs: slice.map do |record|
              {
                id: fetch_crm_id(record),
                properties: stringify_keys(record[:properties] || record["properties"] || {}),
              }
            end,
          }

          resp = request(:post, path, body: body)
          raise_for_error!(resp, path: path)

          # crm_id is the input key and does not change on update: build the
          # mapping from the inputs rather than parsing the response body.
          slice.each do |record|
            id = fetch_crm_id(record)
            mapping[id] = id unless id.empty?
          end
        end
      end

      # Batch delete (archive) via HubSpot's native /batch/archive endpoint.
      # @param object_type [String] CRM object type
      # @param crm_ids [Array<String>] hs_object_id values to archive
      # @return [Boolean] true when all batches succeed
      def batch_delete!(object_type:, crm_ids:)
        raise ArgumentError, "object_type must be a String" if !object_type.is_a?(String) || object_type.empty?
        raise ArgumentError, "crm_ids must be a non-empty Array" if !crm_ids.is_a?(Array) || crm_ids.empty?

        path = "/crm/v3/objects/#{object_type}/batch/archive"

        crm_ids.each_slice(BATCH_MAX_SIZE) do |slice|
          body = {
            inputs: slice.map { |id| {id: id.to_s} },
          }

          resp = request(:post, path, body: body)
          raise_for_error!(resp, path: path)
        end

        true
      end

      private

      def request(method, path, body: nil, query: {})
        @rate_limiter&.throttle!

        url = API_BASE + path
        url += "?#{URI.encode_www_form(query)}" unless query.empty?

        headers = {
          "Authorization" => "Bearer #{@access_token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
        }

        raw_body = body && JSON.dump(body)

        begin
          res = @http.request(method, url, headers: headers, body: raw_body)
        rescue => e
          # Normalize all transport errors into TransportError with as much context as possible
          raise Etlify::TransportError.new(
            "HTTP transport error: #{e.class}: #{e.message}",
            status: 0,
            raw: nil
          )
        end

        res[:json] = parse_json_safe(res[:body])
        res
      end

      # Centralized error raising based on status + HubSpot error shape
      def raise_for_error!(resp, path:)
        status = resp[:status].to_i
        return if status.between?(200, 299)

        payload = resp[:json].is_a?(Hash) ? resp[:json] : {}
        # HubSpot error payload commonly includes: message, category, correlationId, context, errors
        message        = payload["message"] || "HubSpot API request failed"
        category       = payload["category"]
        correlation_id = payload["correlationId"]
        details        = payload["errors"] || payload["context"]
        code           = payload["status"] || payload["errorType"] || category

        full_message = "#{message} (status=#{status}, path=#{path}"
        full_message << ", category=#{category}" if category
        full_message << ", correlationId=#{correlation_id}" if correlation_id
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
          code: code,
          category: category,
          correlation_id: correlation_id,
          details: details,
          raw: resp[:body]
        )
      end

      def parse_json_safe(str)
        return nil if str.nil? || str.empty?

        JSON.parse(str)
      rescue JSON::ParserError
        nil
      end

      def find_object_id_by_property(object_type, property, value)
        path = "/crm/v3/objects/#{object_type}/search"

        # Normalize input for safer matching on HubSpot side
        prop = property.to_s
        value = normalize_match_value(prop, value)

        # Base exact match (works for native/custom objects)
        filter_groups = [
          {
            filters: [
              {propertyName: prop, operator: "EQ", value: value},
            ],
          },
        ]

        # Contacts quirks: search secondary emails and handle "+" edge cases
        if object_type == "contacts" && prop == "email"
          # Secondary emails live in hs_additional_emails
          filter_groups << {
            filters: [
              {
                propertyName: "hs_additional_emails",
                operator: "CONTAINS_TOKEN",
                value: value,
              },
            ],
          }

          # Last-resort: try with %2B for APIs that mishandle "+"
          if value.include?("+")
            filter_groups << {
              filters: [
                {
                  propertyName: "email",
                  operator: "EQ",
                  value: value.gsub("+", "%2B"),
                },
              ],
            }
          end
        end

        body = {filterGroups: filter_groups, properties: ["hs_object_id"], limit: 1}
        resp = request(:post, path, body: body)

        if resp[:status] == 200 && resp[:json].is_a?(Hash)
          results = resp[:json]["results"]
          return results.first["id"] if results.is_a?(Array) && results.any?

          return nil
        end

        return nil if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      def update_object(object_type, object_id, properties)
        path = "/crm/v3/objects/#{object_type}/#{object_id}"
        body = {properties: stringify_keys(properties)}
        resp = request(:patch, path, body: body)
        raise_for_error!(resp, path: path)
        true
      end

      def create_object(object_type, properties, match_property, match_value)
        path  = "/crm/v3/objects/#{object_type}"
        props = stringify_keys(properties)

        # The matching property is only written at creation time, and only
        # when the payload does not already carry it (payload wins).
        if !match_value.empty? && !props.key?(match_property)
          props[match_property] = match_value
        end

        props["email"] = props["email"].downcase if props["email"].is_a?(String)

        resp = request(:post, path, body: {properties: props})
        if resp[:status].between?(200, 299) && resp[:json].is_a?(Hash) && resp[:json]["id"]
          return resp[:json]["id"].to_s
        end

        raise_for_error!(resp, path: path)
      end

      # Map each upserted object back to its match value. Values are
      # normalized on both sides (HubSpot lowercases emails in responses)
      # so callers can look results up with their own normalized value.
      def extract_batch_mapping(resp, match_property)
        results = resp[:json].is_a?(Hash) ? resp[:json]["results"] : nil
        return {} unless results.is_a?(Array)

        results.each_with_object({}) do |r, h|
          crm_id = r["id"].to_s
          props = r["properties"] || {}
          match_value = normalize_match_value(match_property, props[match_property])
          h[match_value] = crm_id unless match_value.empty?
        end
      end

      # Strip the value; emails are also lowercased to match HubSpot's
      # canonical form (responses and search are case-insensitive).
      def normalize_match_value(match_property, value)
        clean = value.to_s.strip
        (match_property == "email") ? clean.downcase : clean
      end

      def stringify_keys(hash)
        hash.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      end

      def fetch_crm_id(record)
        (record[:crm_id] || record["crm_id"]).to_s
      end

      def fetch_input(input, key)
        input[key] || input[key.to_s]
      end
    end
  end
end
