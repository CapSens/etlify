require_relative "default_http"
require_relative "brevo/client"

module Etlify
  module Adapters
    # Brevo Adapter (API v3) for contacts, companies, and deals.
    #
    # Usage:
    #   adapter = Etlify::Adapters::BrevoAdapter.new(
    #     api_key: ENV["BREVO_API_KEY"],
    #   )
    #   adapter.upsert!(object_type: "contacts", payload: {email: "john@example.com"}, id_property: :email)
    #   adapter.delete!(object_type: "contacts", crm_id: "42")
    class BrevoAdapter
      IDENTIFIER_TYPE_MAP = {
        "email" => "email_id",
        "ext_id" => "ext_id",
        "phone" => "phone_id",
        "sms" => "phone_id",
      }.freeze

      OBJECT_PATHS = {
        "contacts" => "/contacts",
        "companies" => "/companies",
        "deals" => "/crm/deals",
      }.freeze

      def initialize(api_key:, http_client: nil)
        validate_string!(:api_key, api_key)

        @client = Brevo::Client.new(
          api_key: api_key,
          http: http_client || DefaultHttp.new
        )
      end

      def rate_limiter
        @client.rate_limiter
      end

      def rate_limiter=(limiter)
        @client.rate_limiter = limiter
      end

      # Etlify adapter interface.
      # @return [String] CRM ID
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        validate_object_type!(object_type)
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)

        properties = stringify_keys(payload)

        object_id = resolve_object_id(
          object_type, properties, id_property, crm_id
        )

        if object_id
          update_record(object_type, object_id, properties)
          object_id.to_s
        else
          create_record(object_type, properties)
        end
      end

      # Etlify adapter interface.
      # @return [Boolean]
      def delete!(object_type:, crm_id:)
        validate_object_type!(object_type)
        raise ArgumentError, "crm_id must be provided" if crm_id.nil? || crm_id.to_s.empty?

        path = "#{base_path(object_type)}/#{crm_id}"
        response = @client.delete(path)

        return true if response[:status].between?(200, 299)
        return false if response[:status] == 404

        @client.raise_for_error!(response, path: path)
      end

      private

      def base_path(object_type)
        OBJECT_PATHS.fetch(object_type) do
          message = [
            "Unsupported object_type: #{object_type.inspect}.",
            "Supported: #{OBJECT_PATHS.keys.join(', ')}",
          ].join(" ")
          raise ArgumentError, message
        end
      end

      def resolve_object_id(object_type, properties, id_property, crm_id)
        unless crm_id.to_s.strip.empty?
          return crm_id.to_s.strip
        end

        return nil unless id_property

        key = id_property.to_s
        value = properties[key]
        return nil unless value

        find_record(object_type, key, value)
      end

      def find_record(object_type, id_property, value)
        case object_type
        when "contacts"
          find_contact(id_property, value)
        when "companies", "deals"
          nil
        end
      end

      def find_contact(id_property, value)
        identifier_type = IDENTIFIER_TYPE_MAP[id_property] || "email_id"
        path = "/contacts/#{URI.encode_www_form_component(value.to_s)}"

        response = @client.get(
          path,
          query: {"identifierType" => identifier_type}
        )

        if response[:status] == 200 && response[:json].is_a?(Hash)
          return response[:json]["id"].to_s
        end

        return nil if response[:status] == 404

        @client.raise_for_error!(response, path: path)
      end

      def create_record(object_type, properties)
        path = base_path(object_type)

        body = case object_type
        when "contacts"
          build_contact_body(properties)
        when "companies"
          build_company_body(properties)
        when "deals"
          build_deal_body(properties)
        end

        response = @client.post(path, body: body)
        @client.raise_for_error!(response, path: path)

        extract_id(response)
      end

      def update_record(object_type, record_id, properties)
        path = "#{base_path(object_type)}/#{record_id}"

        body = case object_type
        when "contacts"
          build_contact_body(properties)
        when "companies"
          build_company_body(properties)
        when "deals"
          build_deal_body(properties)
        end

        response = if object_type == "contacts"
          @client.put(path, body: body)
        else
          @client.patch(path, body: body)
        end

        @client.raise_for_error!(response, path: path)
        true
      end

      def build_contact_body(properties)
        email = properties.delete("email")
        ext_id = properties.delete("ext_id")

        body = {}
        body[:email] = email if email
        body[:ext_id] = ext_id if ext_id
        body[:attributes] = properties if properties.any?
        body
      end

      def build_company_body(properties)
        name = properties.delete("name")
        body = {}
        body[:name] = name if name
        body[:attributes] = properties if properties.any?
        body
      end

      def build_deal_body(properties)
        name = properties.delete("deal_name") || properties.delete("name")
        body = {}
        body[:name] = name if name
        body[:attributes] = properties if properties.any?
        body
      end

      def extract_id(response)
        json = response[:json]
        return nil unless json.is_a?(Hash)

        (json["id"] || json["contactId"]).to_s
      end

      def stringify_keys(hash)
        hash.transform_keys(&:to_s)
      end

      def validate_string!(name, value)
        return if value.is_a?(String) && !value.empty?

        raise ArgumentError, "#{name} must be a non-empty String"
      end

      def validate_object_type!(object_type)
        return if OBJECT_PATHS.key?(object_type)

        message = [
          "Unsupported object_type: #{object_type.inspect}.",
          "Supported: #{OBJECT_PATHS.keys.join(', ')}",
        ].join(" ")
        raise ArgumentError, message
      end
    end
  end
end
