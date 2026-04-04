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
    # Usage:
    #   adapter = Etlify::Adapters::HubspotV3Adapter.new(access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"])
    #   adapter.upsert!(object_type: "contacts", payload: {email: "john@example.com"}, id_property: :email, crm_id: nil)
    #   adapter.delete!(object_type: "contacts", crm_id: "123") # => true, or false if 404
    class HubspotV3Adapter
      API_BASE = "https://api.hubapi.com"
      BATCH_MAX_SIZE = 100

      # @param access_token [String] HubSpot private app token
      # @param http_client [#request] Optional HTTP client for tests. Signature: request(method, url, headers:, body:)
      def initialize(access_token:, http_client: nil)
        @access_token = access_token
        @http         = http_client || Etlify::Adapters::DefaultHttp.new
      end

      # Upsert by searching on id_property (if provided), otherwise create directly.
      # @param object_type [String] HubSpot CRM object type (e.g., "contacts", "companies", "deals", or a custom object)
      # @param payload [Hash] Properties for the object
      # @param id_property [String, nil] Unique property used to search and upsert
      # @param crm_id [Integer, String, nil] Record's HubSpot hs_object_id if known
      #   (overrides id_property search if provided)
      #   If both crm_id and id_property are nil, a new object is created.
      #   If id_property is provided but not found, a new object is created.
      # (e.g., "email" for contacts, "domain" for companies)
      # @return [String, nil] HubSpot hs_object_id as string or nil if not available
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        properties   = payload.dup
        unique_value = nil

        if crm_id.to_s.strip.empty?
          if id_property
            # Extract unique value whether id_property/payload keys are
            # string or symbol. Normalize, then try both forms.
            key_str = id_property.to_s
            key_sym = key_str.to_sym
            unique_value =
              properties.delete(key_str) || properties.delete(key_sym)
          end

          object_id = if id_property && unique_value
            find_object_id_by_property(object_type, id_property, unique_value)
          end
        else
          object_id = crm_id.to_s.strip
        end

        if object_id
          update_object(object_type, object_id, properties)
          object_id.to_s
        else
          create_object(object_type, properties, id_property, unique_value)
        end
      end

      # Delete an object by hs_object_id.
      # @param object_type [String]
      # @param crm_id [String]
      # @return [Boolean] true on 2xx response, false on 404
      def delete!(object_type:, crm_id:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "crm_id must be provided" if crm_id.nil? || crm_id.to_s.empty?

        path = "/crm/v3/objects/#{object_type}/#{crm_id}"
        resp = request(:delete, path)

        return true if resp[:status].between?(200, 299)
        return false if resp[:status] == 404

        raise_for_error!(resp, path: path)
      end

      # Batch upsert via HubSpot's native /batch/upsert endpoint.
      # @param object_type [String] CRM object type
      # @param records [Array<Hash>] Properties hashes (must include the id_property key)
      # @param id_property [String] Unique property for matching (e.g., "email")
      # @return [Array<String>] hs_object_id strings for each upserted record
      def batch_upsert!(object_type:, records:, id_property:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "id_property must be provided" if id_property.nil? || id_property.to_s.empty?
        raise ArgumentError, "records must be a non-empty Array" unless records.is_a?(Array) && !records.empty?

        path = "/crm/v3/objects/#{object_type}/batch/upsert"

        records.each_slice(BATCH_MAX_SIZE).flat_map do |slice|
          body = {
            inputs: slice.map do |record|
              props = stringify_keys(record)
              id_value = props.delete(id_property.to_s)
              {
                id: id_value.to_s,
                idProperty: id_property.to_s,
                properties: props,
              }
            end,
          }

          resp = request(:post, path, body: body)
          raise_for_error!(resp, path: path)
          extract_batch_ids(resp)
        end
      end

      # Batch delete (archive) via HubSpot's native /batch/archive endpoint.
      # @param object_type [String] CRM object type
      # @param crm_ids [Array<String>] hs_object_id values to archive
      # @return [Boolean] true when all batches succeed
      def batch_delete!(object_type:, crm_ids:)
        raise ArgumentError, "object_type must be a String" unless object_type.is_a?(String) && !object_type.empty?
        raise ArgumentError, "crm_ids must be a non-empty Array" unless crm_ids.is_a?(Array) && !crm_ids.empty?

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
        clean_value = value.to_s.strip
        value = (prop == "email") ? clean_value.downcase : clean_value

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

      def create_object(object_type, properties, id_property, unique_value)
        path  = "/crm/v3/objects/#{object_type}"
        props = stringify_keys(properties)

        # If a unique property was provided and its value was extracted, ensure it is present on creation
        if id_property && unique_value && !props.key?(id_property.to_s)
          props[id_property.to_s] = unique_value
        end

        props["email"] = props["email"].downcase if props.key?("email")

        resp = request(:post, path, body: {properties: props})
        if resp[:status].between?(200, 299) && resp[:json].is_a?(Hash) && resp[:json]["id"]
          return resp[:json]["id"].to_s
        end

        raise_for_error!(resp, path: path)
      end

      def extract_batch_ids(resp)
        results = resp[:json].is_a?(Hash) ? resp[:json]["results"] : nil
        return [] unless results.is_a?(Array)

        results.map { |r| r["id"].to_s }
      end

      def stringify_keys(hash)
        hash.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      end
    end
  end
end
