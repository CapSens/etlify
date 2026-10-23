require "erb"
require_relative "default_http"
require_relative "brevo/client"

module Etlify
  module Adapters
    class BrevoAdapter
      OBJECT_TYPES = ["contacts"].freeze
      IDENTIFIER_TYPES = {
        "email" => "email_id",
        "ext_id" => "ext_id",
      }.freeze
      EVENT_IDENTIFIERS = {
        "email" => "email_id",
        "ext_id" => "ext_id",
        "crm_id" => "contact_id",
      }.freeze
      EVENT_NAME_FORMAT = /\A[A-Za-z0-9_-]{1,255}\z/.freeze

      def rate_limiter
        @client.rate_limiter
      end

      def rate_limiter=(limiter)
        @client.rate_limiter = limiter
      end

      def initialize(api_key:, http_client: nil)
        if !api_key.is_a?(String) || api_key.strip.empty?
          raise ArgumentError, "api_key must be a non-empty String"
        end

        @client = Brevo::Client.new(
          api_key: api_key,
          http: http_client || DefaultHttp.new
        )
      end

      def upsert!(object_type:, payload:, match_property:, match_value:, crm_id: nil)
        validate_object_type!(object_type)
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        prop = match_property.to_s
        unless IDENTIFIER_TYPES.key?(prop)
          raise ArgumentError,
                "match_property must be one of #{IDENTIFIER_TYPES.keys.inspect}"
        end

        value = normalize_match_value(prop, match_value)

        contact_id = if crm_id.to_s.strip.empty?
          if value.empty?
            raise ArgumentError,
                  "match_value must be provided when crm_id is unknown"
          end

          find_contact_id(prop, value)
        else
          crm_id.to_s.strip
        end

        properties = stringify_keys(payload)
        normalize_email!(properties)

        if contact_id
          begin
            update_contact(contact_id, properties)
            contact_id
          rescue Etlify::NotFound
            raise if crm_id.to_s.strip.empty? || value.empty?

            upsert_by_match(properties, prop, value)
          end
        else
          create_contact(properties, prop, value)
        end
      end

      def delete!(object_type:, crm_id:)
        validate_object_type!(object_type)
        if crm_id.to_s.strip.empty?
          raise ArgumentError, "crm_id must be provided"
        end

        path = "/contacts/#{encode(crm_id.to_s.strip)}"
        response = @client.delete(path, query: {identifierType: "contact_id"})

        return true if response[:status].between?(200, 299)
        return false if response[:status] == 404

        @client.raise_for_error!(response, path: path)
      end

      def batch_delete!(object_type:, crm_ids:)
        validate_object_type!(object_type)
        if !crm_ids.is_a?(Array) || crm_ids.empty?
          raise ArgumentError, "crm_ids must be a non-empty Array"
        end

        crm_ids.each do |id|
          delete!(object_type: object_type, crm_id: id.to_s)
        end

        true
      end

      def track_event!(
        name:,
        identifier_property:,
        identifier_value:,
        properties: {},
        contact_properties: {},
        occurred_at: nil
      )
        event_name = name.to_s
        unless EVENT_NAME_FORMAT.match?(event_name)
          raise ArgumentError,
                "name must be 1 to 255 alphanumeric, '-' or '_' characters"
        end

        identifier_key = EVENT_IDENTIFIERS[identifier_property.to_s]
        unless identifier_key
          raise ArgumentError,
                "identifier_property must be one of " \
                "#{EVENT_IDENTIFIERS.keys.inspect}"
        end

        body = {
          "event_name" => event_name,
          "identifiers" => {
            identifier_key => event_identifier_value(
              identifier_property.to_s, identifier_value
            ),
          },
        }
        unless properties.nil? || properties.empty?
          body["event_properties"] = stringify_keys(properties)
        end
        unless contact_properties.nil? || contact_properties.empty?
          body["contact_properties"] = stringify_keys(contact_properties)
        end
        body["event_date"] = format_time(occurred_at) if occurred_at

        path = "/events"
        response = @client.post(path, body: body)
        @client.raise_for_error!(response, path: path)
        true
      end

      private

      def find_contact_id(match_property, match_value)
        path = "/contacts/#{encode(match_value)}"
        response = @client.get(
          path, query: {identifierType: IDENTIFIER_TYPES.fetch(match_property)}
        )

        if response[:status] == 200 && response[:json].is_a?(Hash)
          id = response[:json]["id"]
          return id&.to_s
        end

        return nil if response[:status] == 404

        @client.raise_for_error!(response, path: path)
      end

      def upsert_by_match(properties, match_property, match_value)
        contact_id = find_contact_id(match_property, match_value)

        if contact_id
          update_contact(contact_id, properties)
          contact_id
        else
          create_contact(properties, match_property, match_value)
        end
      end

      def update_contact(contact_id, properties)
        path = "/contacts/#{encode(contact_id)}"
        response = @client.put(
          path, body: properties, query: {identifierType: "contact_id"}
        )
        @client.raise_for_error!(response, path: path)
        true
      end

      def create_contact(properties, match_property, match_value)
        if !match_value.empty? && !properties.key?(match_property)
          properties = properties.merge(match_property => match_value)
        end

        path = "/contacts"
        response = @client.post(path, body: properties)
        if response[:status].between?(200, 299) &&
            response[:json].is_a?(Hash) && response[:json]["id"]
          return response[:json]["id"].to_s
        end

        @client.raise_for_error!(response, path: path)
      end

      def normalize_email!(properties)
        return unless properties["email"].is_a?(String)

        properties["email"] = properties["email"].downcase
      end

      def event_identifier_value(identifier_property, value)
        clean = value.to_s.strip
        if clean.empty?
          raise ArgumentError, "identifier_value must be provided"
        end

        case identifier_property
        when "crm_id" then Integer(clean, 10)
        when "email" then clean.downcase
        else clean
        end
      end

      def normalize_match_value(match_property, value)
        clean = value.to_s.strip
        (match_property == "email") ? clean.downcase : clean
      end

      def validate_object_type!(object_type)
        return if OBJECT_TYPES.include?(object_type.to_s)

        raise ArgumentError,
              "object_type must be one of #{OBJECT_TYPES.inspect}"
      end

      def format_time(value)
        value.is_a?(String) ? value : value.iso8601
      end

      def encode(segment)
        ERB::Util.url_encode(segment)
      end

      def stringify_keys(hash)
        hash.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      end
    end
  end
end
