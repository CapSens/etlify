require "json"
require "uri"
require_relative "default_http"
require_relative "zoho_crm/access_token"

module Etlify
  module Adapters
    # Zoho CRM Adapter (REST API v8) with per-call module.
    # The object_type is the module API name (e.g. "Contacts", "Leads",
    # "Accounts", or a custom module such as "Investors").
    #
    # Authentication: OAuth 2.0 refresh token flow (Zoho access tokens expire
    # after one hour). See ZohoCrm::AccessToken.
    #
    # Matching: records without a crm_id go through Zoho's native upsert
    # endpoint with match_property as the only duplicate check field, so the
    # lookup happens server side. The search API is deliberately not used:
    # its index lags behind writes, and a record created a moment earlier
    # would not be found and would be created twice. Consequence: the
    # matching field must be one Zoho accepts as a duplicate check field
    # (a system unique field such as Email, or a field marked "Do not allow
    # duplicate values").
    #
    # Matching contract: the payload is synced as-is. The match field is
    # injected only when the payload does not carry it, since the upsert
    # endpoint reads the duplicate check value from the record itself. On a
    # matched record this rewrites the value it was matched on.
    #
    # Error handling:
    # - Zoho reports failures per record inside `data`, sometimes behind a 2xx
    #   status. Any failed record raises ValidationFailed.
    # - Other non-2xx responses raise Unauthorized, NotFound, RateLimited,
    #   ValidationFailed or ApiError.
    # - Transport-level issues raise TransportError.
    # - delete! returns false when the record does not exist.
    #
    # Usage:
    #   adapter = Etlify::Adapters::ZohoCrmAdapter.new(
    #     client_id: ENV["ZOHO_CLIENT_ID"],
    #     client_secret: ENV["ZOHO_CLIENT_SECRET"],
    #     refresh_token: ENV["ZOHO_REFRESH_TOKEN"],
    #     data_center: :eu,
    #   )
    #   adapter.upsert!(
    #     object_type: "Contacts",
    #     payload: {Last_Name: "Doe"},
    #     match_property: "Email",
    #     match_value: "john@example.com"
    #   )
    #   adapter.delete!(object_type: "Contacts", crm_id: "5725767000000524157")
    class ZohoCrmAdapter
      DATA_CENTERS = {
        us: {accounts_url: "https://accounts.zoho.com", api_url: "https://www.zohoapis.com"},
        eu: {accounts_url: "https://accounts.zoho.eu", api_url: "https://www.zohoapis.eu"},
        in: {accounts_url: "https://accounts.zoho.in", api_url: "https://www.zohoapis.in"},
        au: {accounts_url: "https://accounts.zoho.com.au", api_url: "https://www.zohoapis.com.au"},
        jp: {accounts_url: "https://accounts.zoho.jp", api_url: "https://www.zohoapis.jp"},
        ca: {accounts_url: "https://accounts.zohocloud.ca", api_url: "https://www.zohoapis.ca"},
        cn: {accounts_url: "https://accounts.zoho.com.cn", api_url: "https://www.zohoapis.com.cn"},
        sa: {accounts_url: "https://accounts.zoho.sa", api_url: "https://www.zohoapis.sa"},
      }.freeze
      DEFAULT_API_VERSION = "v8"
      BATCH_MAX_SIZE = 100
      INVALID_TOKEN_CODE = "INVALID_TOKEN"
      # Code Zoho returns for an id that matches no record.
      INVALID_DATA_CODE = "INVALID_DATA"

      attr_accessor :rate_limiter

      # @param client_id [String] OAuth client id (Zoho API Console)
      # @param client_secret [String] OAuth client secret
      # @param refresh_token [String] long-lived refresh token
      # @param data_center [Symbol] one of DATA_CENTERS keys (default :us)
      # @param api_version [String] CRM API version (default "v8")
      # @param token_cache [ActiveSupport::Cache::Store, false] store sharing
      #   the access token across processes; defaults to
      #   Etlify.config.cache_store, false keeps it per process
      # @param http_client [#request] Optional HTTP client for tests.
      #   Signature: request(method, url, headers:, body:)
      def initialize(
        client_id:,
        client_secret:,
        refresh_token:,
        data_center: :us,
        api_version: DEFAULT_API_VERSION,
        token_cache: Etlify.config.cache_store,
        http_client: nil
      )
        unless DATA_CENTERS.key?(data_center)
          raise ArgumentError, "data_center must be one of #{DATA_CENTERS.keys.inspect}"
        end

        validate_string!(:client_id, client_id)
        validate_string!(:client_secret, client_secret)
        validate_string!(:refresh_token, refresh_token)

        urls = DATA_CENTERS.fetch(data_center)
        @api_base = "#{urls.fetch(:api_url)}/crm/#{api_version}"
        @http = http_client || Etlify::Adapters::DefaultHttp.new
        @access_token = ZohoCrm::AccessToken.new(
          accounts_url: urls.fetch(:accounts_url),
          client_id: client_id,
          client_secret: client_secret,
          refresh_token: refresh_token,
          http: @http,
          cache: token_cache || nil
        )
      end

      # Keeps the credentials held by the access token out of consoles, logs
      # and error reports.
      def inspect
        "#<#{self.class.name} api_base=#{@api_base.inspect}>"
      end

      # Update by crm_id when known, otherwise upsert on match_property.
      # @param object_type [String] Zoho module API name
      # @param payload [Hash] Fields keyed by API name, synced as-is
      # @param match_property [String, Symbol] duplicate check field API name
      # @param match_value [String] Value of match_property for this record
      # @param crm_id [String, nil] Zoho record id if already known
      # @return [String] Zoho record id
      def upsert!(object_type:, payload:, match_property:, match_value:, crm_id: nil)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "payload must be a Hash" unless payload.is_a?(Hash)
        raise ArgumentError, "match_property must be provided" if match_property.to_s.strip.empty?

        known_id = crm_id.to_s.strip
        unless known_id.empty?
          update_record(object_type, known_id, payload)
          return known_id
        end

        value = match_value.to_s.strip
        raise ArgumentError, "match_value must be provided when crm_id is unknown" if value.empty?

        prop = match_property.to_s
        path = "/#{object_type}/upsert"
        ids, response = upsert_records(path, [upsert_fields(payload, prop, value)], prop)
        return ids.first unless ids.first.nil?

        raise Etlify::ApiError.new(
          "Zoho upsert succeeded but returned no record id (path=#{path})",
          status: response[:status],
          raw: response[:body]
        )
      end

      # @return [Boolean] true when deleted, false when the record does not exist
      def delete!(object_type:, crm_id:)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "crm_id must be provided" if crm_id.to_s.strip.empty?

        path = "/#{object_type}/#{crm_id}"
        response = request(:delete, path)
        return false if response[:status] == 404
        return false if record_entries(response).any? { |entry| missing_record?(entry) }

        raise_for_error!(response, path: path)
        true
      end

      # Batch upsert via Zoho's native /upsert endpoint (100 records per call).
      # Zoho answers in input order, so ids are mapped back by position.
      # A failed record raises ValidationFailed: the batch synchronizer then
      # replays the records one by one, which is safe since matching is done
      # server side. A record Zoho returned no id for is left out of the
      # mapping, so only that record is failed by the batch synchronizer.
      # @param inputs [Array<Hash>] each {value:, properties:}
      # @param match_property [String] duplicate check field API name
      # @return [Hash{String => String}] input value (as provided) => record id
      def batch_upsert!(object_type:, inputs:, match_property:)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "match_property must be provided" if match_property.to_s.strip.empty?
        raise ArgumentError, "inputs must be a non-empty Array" if !inputs.is_a?(Array) || inputs.empty?

        prop = match_property.to_s
        path = "/#{object_type}/upsert"

        inputs.each_slice(BATCH_MAX_SIZE).each_with_object({}) do |slice, mapping|
          raw_values = slice.map { |input| fetch_input(input, :value).to_s }
          records = slice.each_with_index.map do |input, index|
            value = raw_values[index].strip
            raise ArgumentError, "every input must carry a non-blank :value" if value.empty?

            upsert_fields(fetch_input(input, :properties) || {}, prop, value)
          end

          ids, _response = upsert_records(path, records, prop)
          raw_values.each_with_index { |raw, index| mapping[raw] = ids[index] unless ids[index].nil? }
        end
      end

      # Batch update by known record id (100 records per call).
      # @param records [Array<Hash>] each {crm_id:, properties:}
      # @return [Hash{String => String}] identity mapping {crm_id => crm_id}
      def batch_update!(object_type:, records:)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "records must be a non-empty Array" if !records.is_a?(Array) || records.empty?

        path = "/#{object_type}"

        records.each_slice(BATCH_MAX_SIZE).each_with_object({}) do |slice, mapping|
          data = slice.map do |record|
            stringify_keys(fetch_input(record, :properties) || {}).merge("id" => fetch_crm_id(record))
          end

          response = request(:put, path, body: {data: data})
          raise_for_error!(response, path: path)

          slice.each do |record|
            id = fetch_crm_id(record)
            mapping[id] = id unless id.empty?
          end
        end
      end

      # Batch delete by record id (100 ids per call). Ids matching no record
      # are treated as already deleted.
      # @return [Boolean] true when all batches succeed
      def batch_delete!(object_type:, crm_ids:)
        validate_string!(:object_type, object_type)
        raise ArgumentError, "crm_ids must be a non-empty Array" if !crm_ids.is_a?(Array) || crm_ids.empty?

        path = "/#{object_type}"

        crm_ids.each_slice(BATCH_MAX_SIZE) do |slice|
          response = request(:delete, path, query: {ids: slice.map(&:to_s).join(",")})
          raise_for_error!(response, path: path, tolerate_missing_records: true)
        end

        true
      end

      private

      def update_record(object_type, record_id, payload)
        path = "/#{object_type}/#{record_id}"
        response = request(:put, path, body: {data: [stringify_keys(payload)]})
        raise_for_error!(response, path: path)
      end

      # @return [Array(Array<String, nil>, Hash)] record ids in the order of
      #   `records` (nil where Zoho returned none) and the response
      def upsert_records(path, records, match_property)
        body = {data: records, duplicate_check_fields: [match_property]}
        response = request(:post, path, body: body)
        raise_for_error!(response, path: path)

        entries = record_entries(response)
        ids = records.each_index.map do |index|
          id = entries.dig(index, "details", "id").to_s
          id.empty? ? nil : id
        end

        [ids, response]
      end

      # The upsert endpoint reads the duplicate check value from the record:
      # inject it unless the payload already carries it (payload wins).
      def upsert_fields(payload, match_property, match_value)
        fields = stringify_keys(payload)
        fields[match_property] = match_value unless fields.key?(match_property)
        fields
      end

      # Retries once with a new token when Zoho rejects the current one
      # (revoked, or expired earlier than announced).
      def request(method, path, body: nil, query: {})
        token = @access_token.value
        response = perform_request(method, path, token, body: body, query: query)
        return response unless response[:status] == 401 && error_code(response) == INVALID_TOKEN_CODE

        @access_token.invalidate!(token)
        perform_request(method, path, @access_token.value, body: body, query: query)
      end

      def perform_request(method, path, token, body:, query:)
        @rate_limiter&.throttle!

        url = @api_base + path
        url += "?#{URI.encode_www_form(query)}" unless query.empty?

        headers = {
          "Authorization" => "Zoho-oauthtoken #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
        }

        raw_body = body && JSON.dump(body)

        begin
          response = @http.request(method, url, headers: headers, body: raw_body)
        rescue => error_detail
          raise Etlify::TransportError.new(
            "HTTP transport error: #{error_detail.class}: #{error_detail.message}",
            status: 0,
            raw: nil
          )
        end

        response[:json] = parse_json_safe(response[:body])
        response
      end

      # Zoho returns {"code", "message", "details", "status"} either at the
      # top level (request-wide failure) or for each record inside `data`.
      # With tolerate_missing_records, ids matching no record count as done,
      # even when every entry is one of them and Zoho answers with a 4xx.
      def raise_for_error!(response, path:, tolerate_missing_records: false)
        status = response[:status].to_i
        entries = record_entries(response)
        failed_entry = entries.find do |entry|
          entry["status"] == "error" && !(tolerate_missing_records && missing_record?(entry))
        end
        return if failed_entry.nil? && status.between?(200, 299)
        return if failed_entry.nil? && tolerate_missing_records && !entries.empty?

        error = failed_entry || (response[:json].is_a?(Hash) ? response[:json] : {})
        code = error["code"]
        details = error["details"]

        full_message = "#{error['message'] || 'Zoho CRM API request failed'} (status=#{status}, path=#{path}"
        full_message << ", code=#{code}" if code
        full_message << ", details=#{JSON.dump(details)}" if details.is_a?(Hash) && !details.empty?
        full_message << ")"

        klass =
          if failed_entry
            Etlify::ValidationFailed
          else
            case status
            when 401, 403 then Etlify::Unauthorized
            when 404      then Etlify::NotFound
            when 409, 422 then Etlify::ValidationFailed
            when 429      then Etlify::RateLimited
            else Etlify::ApiError
            end
          end

        raise klass.new(
          full_message,
          status: status,
          code: code,
          details: details,
          raw: response[:body]
        )
      end

      def record_entries(response)
        data = response[:json].is_a?(Hash) ? response[:json]["data"] : nil
        return [] unless data.is_a?(Array)

        data.select { |entry| entry.is_a?(Hash) }
      end

      def missing_record?(entry)
        entry["status"] == "error" && entry["code"] == INVALID_DATA_CODE
      end

      def error_code(response)
        response[:json].is_a?(Hash) ? response[:json]["code"] : nil
      end

      def parse_json_safe(raw_body)
        return nil if raw_body.nil? || raw_body.empty?

        JSON.parse(raw_body)
      rescue JSON::ParserError
        nil
      end

      def stringify_keys(hash)
        hash.transform_keys(&:to_s)
      end

      def fetch_crm_id(record)
        fetch_input(record, :crm_id).to_s
      end

      def fetch_input(input, key)
        input[key] || input[key.to_s]
      end

      def validate_string!(name, value)
        return if value.is_a?(String) && !value.empty?

        raise ArgumentError, "#{name} must be a non-empty String"
      end
    end
  end
end
