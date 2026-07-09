require "json"
require "uri"
require "net/http"
require_relative "default_http"

module Etlify
  module Adapters
    # Intercom Adapter (REST API) with per-call object type.
    # The object_type is interpolated directly in the URL, mirroring the
    # HubSpot adapter convention. Intercom does not provide native batch
    # endpoints, so batch_delete! loops over the single-record delete!
    # (the Etlify rate limiter throttles each HTTP call). The adapter
    # deliberately does not implement batch_upsert!: BatchSyncJob detects
    # this and falls back to sequential per-record synchronization.
    #
    # Error handling:
    # - Non-2xx responses raise specific exceptions (Unauthorized, NotFound,
    #   RateLimited, ValidationFailed, ApiError).
    # - Transport-level issues raise TransportError.
    # - delete! returns false on 404 (object not found), raises otherwise.
    #
    # Matching contract: the payload is synced as-is. `match_property` /
    # `match_value` are only used to find the object (or to set the property
    # at creation time when the payload does not include it). The matching
    # property is NEVER written on an existing object unless the payload
    # explicitly includes it.
    #
    # Usage:
    #   adapter = Etlify::Adapters::IntercomAdapter.new(
    #     access_token: ENV["INTERCOM_ACCESS_TOKEN"],
    #     region: :eu,
    #   )
    #   adapter.upsert!(
    #     object_type: "contacts",
    #     payload: {name: "John"},
    #     match_property: "email",
    #     match_value: "john@example.com",
    #     crm_id: nil
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

      # Upsert by crm_id when known, otherwise search on match_property /
      # match_value, otherwise create.
      # @param object_type [String] Intercom resource (e.g. "contacts", "companies")
      # @param payload [Hash] Attributes for the object, synced as-is
      # @param match_property [String, Symbol] Unique property used to search
      #   (e.g. "email" or "external_id" for contacts)
      # @param match_value [String] Value of match_property for this record
      # @param crm_id [String, nil] Intercom id if already known (skips the
      #   match_property search entirely)
      # @return [String, nil] Intercom id as string or nil if not available
      def upsert!(object_type:, payload:, match_property:, match_value:, crm_id: nil)
        if !object_type.is_a?(String) || object_type.empty?
          raise ArgumentError, "object_type must be a String"
        end
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)
        if match_property.to_s.strip.empty?
          raise ArgumentError, "match_property must be provided"
        end

        prop  = match_property.to_s
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

        properties = stringify_keys(payload)
        normalize_email!(object_type, properties)

        if object_id
          update_object(object_type, object_id, properties)
          object_id.to_s
        else
          create_object(object_type, properties, prop, value)
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

      def create_object(object_type, properties, match_property, match_value)
        # The matching property is only written at creation time, and only
        # when the payload does not already carry it (payload wins).
        if !match_value.empty? && !properties.key?(match_property)
          properties = properties.merge(match_property => match_value)
        end

        path = "/#{object_type}"
        response = request(:post, path, body: properties)
        if response[:status].between?(200, 299) &&
            response[:json].is_a?(Hash) && response[:json]["id"]
          return response[:json]["id"].to_s
        end

        raise_for_error!(response, path: path)
      end

      def normalize_match_value(match_property, value)
        clean = value.to_s.strip
        (match_property == "email") ? clean.downcase : clean
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
