require "rails_helper"
require "etlify/adapters/dynamics_365/client"
require "etlify/adapters/dynamics_365/token_cache"

RSpec.describe Etlify::Adapters::Dynamics365::Client do
  let(:tenant_id)     { "tenant-uuid" }
  let(:client_id)     { "client-uuid" }
  let(:client_secret) { "secret-value" }
  let(:resource_uri)  { "https://contoso.crm.dynamics.com" }
  let(:api_version)   { "9.2" }
  let(:http)          { instance_double("HttpClient") }
  let(:token_cache)   { instance_double("TokenCache") }

  subject(:client) do
    described_class.new(
      tenant_id: tenant_id,
      client_id: client_id,
      client_secret: client_secret,
      resource_uri: resource_uri,
      api_version: api_version,
      http: http,
      token_cache: token_cache
    )
  end

  let(:api_base) { "https://contoso.crm.dynamics.com/api/data/v9.2" }

  before do
    allow(token_cache).to receive(:fetch).and_return("fake-token")
    allow(token_cache).to receive(:invalidate!)
  end

  def stub_http(status:, body: "", headers: {})
    {status: status, body: body, headers: headers}
  end

  def entity_url(entity_set, key)
    "#{api_base}/#{entity_set}(#{key})"
  end

  def entity_id_header(entity_set, guid)
    {"odata-entityid" => [entity_url(entity_set, guid)]}
  end

  describe "URL building and required headers" do
    it "PATCHes by GUID with all OData headers" do
      expect(http).to receive(:request).with(
        :patch,
        "https://contoso.crm.dynamics.com/api/data/v9.2/contacts(abc-123)",
        headers: hash_including(
          "Authorization" => "Bearer fake-token",
          "Accept" => "application/json",
          "Content-Type" => "application/json; charset=utf-8",
          "OData-MaxVersion" => "4.0",
          "OData-Version" => "4.0",
          "If-None-Match" => "null"
        ),
        body: '{"firstname":"John"}'
      ).and_return(stub_http(status: 204))

      response = client.patch_by_guid(
        "contacts", "abc-123", {firstname: "John"}
      )
      expect(response[:status]).to eq(204)
    end

    it "PATCHes by alternate key with OData escaping for single quotes" do
      expect(http).to receive(:request).with(
        :patch,
        entity_url("contacts", "emailaddress1='o''brian@example.com'"),
        headers: hash_including("Authorization" => "Bearer fake-token"),
        body: anything
      ).and_return(
        stub_http(
          status: 204,
          headers: entity_id_header(
            "contacts", "11111111-2222-3333-4444-555555555555"
          )
        )
      )

      response = client.patch_by_alternate_key(
        "contacts", "emailaddress1", "o'brian@example.com",
        {firstname: "John"}
      )
      expect(response[:crm_id]).to eq("11111111-2222-3333-4444-555555555555")
    end

    it "POSTs to the entity set on create" do
      expect(http).to receive(:request).with(
        :post,
        "#{api_base}/contacts",
        headers: hash_including("Authorization" => "Bearer fake-token"),
        body: '{"firstname":"John"}'
      ).and_return(
        stub_http(
          status: 204,
          headers: {
            "OData-EntityId" => [
              entity_url("contacts", "99999999-9999-9999-9999-999999999999"),
            ],
          }
        )
      )

      response = client.post_create("contacts", {firstname: "John"})
      expect(response[:crm_id]).to eq("99999999-9999-9999-9999-999999999999")
    end

    it "DELETEs by GUID" do
      expect(http).to receive(:request).with(
        :delete,
        "https://contoso.crm.dynamics.com/api/data/v9.2/contacts(abc-123)",
        headers: hash_including("Authorization" => "Bearer fake-token"),
        body: nil
      ).and_return(stub_http(status: 204))

      response = client.delete_by_guid("contacts", "abc-123")
      expect(response[:status]).to eq(204)
    end

    it "returns the 404 response without raising on delete" do
      allow(http).to receive(:request).and_return(stub_http(status: 404))

      response = client.delete_by_guid("contacts", "missing")
      expect(response[:status]).to eq(404)
    end

    it "trims trailing slashes on resource_uri" do
      trimmed = described_class.new(
        tenant_id: tenant_id,
        client_id: client_id,
        client_secret: client_secret,
        resource_uri: "https://contoso.crm.dynamics.com//",
        api_version: api_version,
        http: http,
        token_cache: token_cache
      )

      expect(http).to receive(:request).with(
        :patch,
        "https://contoso.crm.dynamics.com/api/data/v9.2/contacts(abc)",
        anything
      ).and_return(stub_http(status: 204))

      trimmed.patch_by_guid("contacts", "abc", {})
    end
  end

  describe "401 retry logic" do
    it "invalidates the cache and retries exactly once on a single 401" do
      call_count = 0
      allow(http).to receive(:request) do
        call_count += 1
        if call_count == 1
          stub_http(
            status: 401,
            body: '{"error":{"code":"0x80048306","message":"Token expired"}}'
          )
        else
          stub_http(status: 204)
        end
      end

      response = client.patch_by_guid("contacts", "abc", {})

      expect(response[:status]).to eq(204)
      expect(call_count).to eq(2)
      expect(token_cache).to have_received(:invalidate!).once
    end

    it "raises Unauthorized when 401 persists after one retry" do
      allow(http).to receive(:request).and_return(
        stub_http(
          status: 401,
          body: '{"error":{"code":"0x80048306","message":"Bad token"}}'
        )
      )

      expect do
        client.patch_by_guid("contacts", "abc", {})
      end.to raise_error(Etlify::Unauthorized, /status=401/)
    end
  end

  describe "OData-EntityId extraction" do
    it "raises ApiError when the header is missing after upsert" do
      allow(http).to receive(:request).and_return(stub_http(status: 204))

      expect do
        client.patch_by_alternate_key(
          "contacts", "emailaddress1", "x@y.z", {}
        )
      end.to raise_error(Etlify::ApiError, /OData-EntityId/)
    end

    it "raises ApiError when the header is malformed" do
      allow(http).to receive(:request).and_return(
        stub_http(
          status: 204,
          headers: {"odata-entityid" => ["not-a-valid-entity-url"]}
        )
      )

      expect do
        client.post_create("contacts", {})
      end.to raise_error(Etlify::ApiError, /Could not extract crm_id/)
    end
  end

  describe "#raise_for_error!" do
    let(:dataverse_error) do
      JSON.dump(
        error: {
          code: "0x80040217",
          message: "An entity with the same key already exists.",
        }
      )
    end

    {
      401 => Etlify::Unauthorized,
      403 => Etlify::Unauthorized,
      404 => Etlify::NotFound,
      409 => Etlify::ValidationFailed,
      422 => Etlify::ValidationFailed,
      429 => Etlify::RateLimited,
      500 => Etlify::ApiError,
      503 => Etlify::ApiError,
    }.each do |status, klass|
      it "maps HTTP #{status} to #{klass}" do
        error = capture_error do
          client.raise_for_error!(
            {
              status: status,
              body: dataverse_error,
              json: JSON.parse(dataverse_error),
              headers: {},
            },
            path: "/whatever"
          )
        end

        expect(error).to be_a(klass)
        expect(error.status).to eq(status)
        expect(error.code).to eq("0x80040217") if status != 404
      end
    end

    it "extracts correlation_id from x-ms-service-request-id header" do
      error = capture_error do
        client.raise_for_error!(
          {
            status: 422,
            body: dataverse_error,
            json: JSON.parse(dataverse_error),
            headers: {"x-ms-service-request-id" => ["req-correlation-uuid"]},
          },
          path: "/x"
        )
      end

      expect(error).to be_a(Etlify::ValidationFailed)
      expect(error.correlation_id).to eq("req-correlation-uuid")
    end

    def capture_error
      yield
      nil
    rescue Etlify::Error => caught
      caught
    end
  end

  describe "transport errors" do
    it "wraps StandardError into TransportError" do
      allow(http).to receive(:request).and_raise(
        StandardError, "ECONNRESET"
      )

      expect do
        client.patch_by_guid("contacts", "abc", {})
      end.to raise_error(Etlify::TransportError, /ECONNRESET/)
    end
  end

  describe "OAuth token fetching" do
    let(:cache) { Etlify::Adapters::Dynamics365::TokenCache.new }
    subject(:real_client) do
      described_class.new(
        tenant_id: tenant_id,
        client_id: client_id,
        client_secret: client_secret,
        resource_uri: resource_uri,
        api_version: api_version,
        http: http,
        token_cache: cache
      )
    end

    it "POSTs to the IdP with form-encoded credentials and parses the token" do
      expect(http).to receive(:request).with(
        :post,
        "https://login.microsoftonline.com/#{tenant_id}/oauth2/v2.0/token",
        headers: hash_including(
          "Content-Type" => "application/x-www-form-urlencoded"
        ),
        body: satisfy do |body|
          params = URI.decode_www_form(body).to_h
          params["grant_type"] == "client_credentials" &&
            params["client_id"] == client_id &&
            params["client_secret"] == client_secret &&
            params["scope"] == "#{resource_uri}/.default"
        end
      ).and_return(
        stub_http(
          status: 200,
          body: JSON.dump(
            access_token: "real-bearer-xyz",
            expires_in: 3599,
            token_type: "Bearer"
          )
        )
      )

      expect(http).to receive(:request).with(
        :patch,
        "https://contoso.crm.dynamics.com/api/data/v9.2/contacts(abc)",
        headers: hash_including("Authorization" => "Bearer real-bearer-xyz"),
        body: anything
      ).and_return(stub_http(status: 204))

      real_client.patch_by_guid("contacts", "abc", {})
    end

    it "raises Unauthorized on invalid_client error" do
      allow(http).to receive(:request).and_return(
        stub_http(
          status: 400,
          body: JSON.dump(
            error: "invalid_client",
            error_description: "AADSTS7000215: Invalid client secret"
          )
        )
      )

      expect do
        real_client.patch_by_guid("contacts", "abc", {})
      end.to raise_error(Etlify::Unauthorized, /invalid_client/)
    end

    it "raises ApiError when the token endpoint returns 500" do
      allow(http).to receive(:request).and_return(
        stub_http(status: 500, body: "")
      )

      expect do
        real_client.patch_by_guid("contacts", "abc", {})
      end.to raise_error(Etlify::ApiError, /OAuth token request failed/)
    end

    it "raises ApiError when the token endpoint returns no access_token" do
      allow(http).to receive(:request).and_return(
        stub_http(status: 200, body: JSON.dump(token_type: "Bearer"))
      )

      expect do
        real_client.patch_by_guid("contacts", "abc", {})
      end.to raise_error(Etlify::ApiError, /access_token/)
    end

    it "raises RateLimited on a 429 from the token endpoint" do
      allow(http).to receive(:request).and_return(
        stub_http(
          status: 429,
          body: JSON.dump(error: "throttled")
        )
      )

      expect do
        real_client.patch_by_guid("contacts", "abc", {})
      end.to raise_error(Etlify::RateLimited)
    end
  end

  describe "JSON parsing edge cases" do
    it "returns a usable response when the body is not valid JSON" do
      allow(http).to receive(:request).and_return(
        stub_http(status: 204, body: "<html>not json</html>",
                  headers: entity_id_header("contacts", "abc-123"))
      )

      response = client.patch_by_alternate_key(
        "contacts", "emailaddress1", "x@y.z", {}
      )
      expect(response[:json]).to be_nil
      expect(response[:crm_id]).to eq("abc-123")
    end
  end

  describe "request URL building with query strings" do
    it "appends query parameters when provided" do
      expect(http).to receive(:request).with(
        :patch,
        "#{entity_url('contacts', 'abc')}?%24select=fullname",
        headers: anything,
        body: anything
      ).and_return(stub_http(status: 204))

      client.send(:request, :patch,
                  "/api/data/v9.2/contacts(abc)",
                  body: {},
                  query: {"$select" => "fullname"})
    end
  end
end
