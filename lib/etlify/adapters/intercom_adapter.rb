require "json"
require "uri"
require "net/http"
require_relative "default_http"

module Etlify
  module Adapters
    # Intercom Adapter (REST API) with per-call object type.
    # The object_type is interpolated directly in the URL, mirroring the
    # HubSpot adapter convention. Intercom does not provide native batch
    # endpoints, so batch_upsert! / batch_delete! loop over the single-record
    # methods (the Etlify rate limiter throttles each HTTP call).
    #
    # Error handling:
    # - Non-2xx responses raise specific exceptions (Unauthorized, NotFound,
    #   RateLimited, ValidationFailed, ApiError).
    # - Transport-level issues raise TransportError.
    # - delete! returns false on 404 (object not found), raises otherwise.
    #
    # Usage:
    #   adapter = Etlify::Adapters::IntercomAdapter.new(
    #     access_token: ENV["INTERCOM_ACCESS_TOKEN"],
    #     region: :eu,
    #   )
    #   adapter.upsert!(
    #     object_type: "contacts",
    #     payload: {email: "john@example.com", external_id: "u_1"},
    #     id_property: "external_id"
    #   )
    #   adapter.delete!(object_type: "contacts", crm_id: "abc123")
    class IntercomAdapter
      REGIONS = {
        us: "https://api.intercom.io",
        eu: "https://api.eu.intercom.io",
        au: "https://api.au.intercom.io",
      }.freeze
      DEFAULT_API_VERSION = "2.14"

      attr_accessor :rate_limiter

      # @param access_token [String] Intercom access token
      # @param region [Symbol] :us (default), :eu or :au
      # @param api_version [String] Intercom-Version header (default "2.14")
      # @param http_client [#request] Optional HTTP client for tests.
      #   Signature: request(method, url, headers:, body:)
      def initialize(
        access_token:,
        region: :us,
        api_version: DEFAULT_API_VERSION,
        http_client: nil
      )
        unless REGIONS.key?(region)
          raise ArgumentError,
                "region must be one of #{REGIONS.keys.inspect}"
        end

        @access_token = access_token
        @api_base     = REGIONS.fetch(region)
        @api_version  = api_version
        @http         = http_client || Etlify::Adapters::DefaultHttp.new
      end

      # Upsert by searching on id_property (if provided), otherwise create
      # directly. If crm_id is provided, skip the search and PUT directly.
      # @param object_type [String] Intercom resource (e.g. "contacts", "companies")
      # @param payload [Hash] Attributes for the object
      # @param id_property [String, nil] Unique property used to search
      # @param crm_id [String, nil] Intercom id if already known
      # @return [String, nil] Intercom id as string or nil if not available
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        if !object_type.is_a?(String) || object_type.empty?
          raise ArgumentError, "object_type must be a String"
        end
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        properties = stringify_keys(payload)
        normalize_email!(object_type, properties)

        if !crm_id.to_s.strip.empty?
          object_id = crm_id.to_s.strip
        elsif id_property
          key = id_property.to_s
          unique_value = properties[key]
          object_id =
            if unique_value
              find_object_id_by_property(object_type, key, unique_value)
            end
        end

        if object_id
          update_object(object_type, object_id, properties)
          object_id.to_s
        else
          create_object(object_type, properties)
        end
      end

      # Delete an object by Intercom id.
      # @param object_type [String]
      # @param crm_id [String]
      # @return [Boolean] true on 2xx, false on 404
      def delete!(object_type:, crm_id:)
        if !object_type.is_a?(String) || object_type.empty?
          raise ArgumentError, "object_type must be a String"
        end
        if crm_id.to_s.strip.empty?
          raise ArgumentError, "crm_id must be provided"
        end

        path = "/#{object_type}/#{crm_id}"
        response = request(:delete, path)

        return true if response[:status].between?(200, 299)
        return false if response[:status] == 404

        raise_for_error!(response, path: path)
      end

      # Sequential batch upsert. Intercom has no native batch endpoint, so
      # this loops over the single-record upsert!.
      # @return [Hash{String => String}] mapping of id_property value to Intercom id
      def batch_upsert!(object_type:, records:, id_property:)
        if !object_type.is_a?(String) || object_type.empty?
          raise ArgumentError, "object_type must be a String"
        end
        if id_property.to_s.empty?
          raise ArgumentError, "id_property must be provided"
        end
        if !records.is_a?(Array) || records.empty?
          raise ArgumentError, "records must be a non-empty Array"
        end

        key = id_property.to_s

        records.each_with_object({}) do |record, mapping|
          properties = stringify_keys(record)
          unique_value = properties[key].to_s
          next if unique_value.empty?

          crm_id = upsert!(
            object_type: object_type,
            payload: record,
            id_property: id_property
          )
          mapping[unique_value] = crm_id.to_s if crm_id
        end
      end

      # Sequential batch delete. Loops over delete! for each id.
      # Raises on the first failure (other than 404, which is treated as
      # already-gone and ignored).
      # @return [Boolean] true when all calls succeeded
      def batch_delete!(object_type:, crm_ids:)
        if !object_type.is_a?(String) || object_type.empty?
          raise ArgumentError, "object_type must be a String"
        end
        if !crm_ids.is_a?(Array) || crm_ids.empty?
          raise ArgumentError, "crm_ids must be a non-empty Array"
        end

        crm_ids.each do |id|
          delete!(object_type: object_type, crm_id: id.to_s)
        end

        true
      end

      private

      def request(method, path, body: nil, query: {})
        @rate_limiter&.throttle!

        url = @api_base + path
        url += "?#{URI.encode_www_form(query)}" unless query.empty?

        headers = {
          "Authorization" => "Bearer #{@access_token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
          "Intercom-Version" => @api_version,
        }

        raw_body = body && JSON.dump(body)

        begin
          response = @http.request(
            method, url, headers: headers, body: raw_body
          )
        rescue => error_detail
          raise Etlify::TransportError.new(
            "HTTP transport error: #{error_detail.class}: " \
              "#{error_detail.message}",
            status: 0,
            raw: nil
          )
        end

        response[:json] = parse_json_safe(response[:body])
        response
      end

      def raise_for_error!(response, path:)
        status = response[:status].to_i
        return if status.between?(200, 299)

        payload = response[:json].is_a?(Hash) ? response[:json] : {}
        errors  = payload["errors"]
        first   = errors.is_a?(Array) ? errors.first : nil
        first   = first.is_a?(Hash) ? first : {}

        message = first["message"] || "Intercom API request failed"
        code    = first["code"]
        field   = first["field"]

        full_message = "#{message} (status=#{status}, path=#{path}"
        full_message << ", code=#{code}" if code
        full_message << ", field=#{field}" if field
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
          category: nil,
          correlation_id: nil,
          details: errors,
          raw: response[:body]
        )
      end

      def parse_json_safe(str)
        return nil if str.nil? || str.empty?

        JSON.parse(str)
      rescue JSON::ParserError
        nil
      end

      def find_object_id_by_property(object_type, property, value)
        path = "/#{object_type}/search"
        body = {
          query: {
            field: property,
            operator: "=",
            value: value.to_s,
          },
        }

        response = request(:post, path, body: body)

        if response[:status] == 200 && response[:json].is_a?(Hash)
          data = response[:json]["data"]
          if data.is_a?(Array) && data.any?
            id = data.first["id"]
            return id.to_s if id
          end
          return nil
        end

        return nil if response[:status] == 404

        raise_for_error!(response, path: path)
      end

      def update_object(object_type, object_id, properties)
        path = "/#{object_type}/#{object_id}"
        response = request(:put, path, body: properties)
        raise_for_error!(response, path: path)
        true
      end

      def create_object(object_type, properties)
        path = "/#{object_type}"
        response = request(:post, path, body: properties)
        if response[:status].between?(200, 299) &&
            response[:json].is_a?(Hash) && response[:json]["id"]
          return response[:json]["id"].to_s
        end

        raise_for_error!(response, path: path)
      end

      def normalize_email!(object_type, properties)
        return unless object_type == "contacts"
        return unless properties.key?("email")
        return unless properties["email"].is_a?(String)

        properties["email"] = properties["email"].downcase
      end

      def stringify_keys(hash)
        hash.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      end
    end
  end
end
