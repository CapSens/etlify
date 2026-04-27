require "rails_helper"
require "etlify/adapters/dynamics_365_adapter"

RSpec.describe Etlify::Adapters::Dynamics365Adapter do
  let(:tenant_id)     { "tenant-uuid" }
  let(:client_id)     { "client-uuid" }
  let(:client_secret) { "secret-value" }
  let(:resource_uri)  { "https://contoso.crm.dynamics.com" }
  let(:http)          { instance_double("HttpClient") }
  let(:token_cache)   { instance_double("TokenCache") }

  subject(:adapter) do
    described_class.new(
      tenant_id: tenant_id,
      client_id: client_id,
      client_secret: client_secret,
      resource_uri: resource_uri,
      http_client: http,
      token_cache: token_cache
    )
  end

  before do
    allow(token_cache).to receive(:fetch).and_return("fake-token")
    allow(token_cache).to receive(:invalidate!)
  end

  let(:api_base) { "https://contoso.crm.dynamics.com/api/data/v9.2" }

  def stub_http(status:, body: "", headers: {})
    {status: status, body: body, headers: headers}
  end

  def entity_url(entity_set, key)
    "#{api_base}/#{entity_set}(#{key})"
  end

  def entity_id_header(entity_set, guid)
    {"odata-entityid" => [entity_url(entity_set, guid)]}
  end

  describe "#initialize" do
    {
      tenant_id: "tenant_id",
      client_id: "client_id",
      client_secret: "client_secret",
      resource_uri: "resource_uri",
    }.each do |arg, name|
      it "raises on blank #{name}" do
        params = {
          tenant_id: tenant_id,
          client_id: client_id,
          client_secret: client_secret,
          resource_uri: resource_uri,
        }
        params[arg] = ""

        expect { described_class.new(**params) }.to raise_error(
          ArgumentError, /#{Regexp.escape(name)}/
        )
      end
    end

    it "raises when resource_uri is not https" do
      expect do
        described_class.new(
          tenant_id: tenant_id,
          client_id: client_id,
          client_secret: client_secret,
          resource_uri: "http://contoso.crm.dynamics.com"
        )
      end.to raise_error(ArgumentError, /https/)
    end

    it "raises when api_version is malformed" do
      expect do
        described_class.new(
          tenant_id: tenant_id,
          client_id: client_id,
          client_secret: client_secret,
          resource_uri: resource_uri,
          api_version: "v9"
        )
      end.to raise_error(ArgumentError, /api_version/)
    end

    it "raises when api_version is blank" do
      expect do
        described_class.new(
          tenant_id: tenant_id,
          client_id: client_id,
          client_secret: client_secret,
          resource_uri: resource_uri,
          api_version: ""
        )
      end.to raise_error(ArgumentError, /api_version/)
    end
  end

  describe "#upsert!" do
    context "with crm_id provided" do
      it "PATCHes by GUID and returns the crm_id verbatim" do
        expect(http).to receive(:request).with(
          :patch,
          "https://contoso.crm.dynamics.com/api/data/v9.2/contacts(abc-123)",
          headers: hash_including("Authorization" => "Bearer fake-token"),
          body: anything
        ).and_return(stub_http(status: 204))

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "John"},
          crm_id: "abc-123"
        )
        expect(result).to eq("abc-123")
      end

      it "ignores id_property when crm_id is also given" do
        allow(http).to receive(:request).and_return(stub_http(status: 204))

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {emailaddress1: "x@y.z"},
          id_property: "emailaddress1",
          crm_id: "abc-123"
        )
        expect(result).to eq("abc-123")
      end

      it "trims whitespace from crm_id" do
        expect(http).to receive(:request).with(
          :patch,
          "https://contoso.crm.dynamics.com/api/data/v9.2/contacts(abc-123)",
          anything
        ).and_return(stub_http(status: 204))

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {},
          crm_id: "  abc-123  "
        )
        expect(result).to eq("abc-123")
      end

      it "falls back to POST create when crm_id is whitespace-only" do
        guid = "55555555-5555-5555-5555-555555555555"
        expect(http).to receive(:request).with(
          :post,
          "https://contoso.crm.dynamics.com/api/data/v9.2/contacts",
          anything
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "John"},
          crm_id: "   "
        )
        expect(result).to eq(guid)
      end

      it "raises NotFound when PATCH by GUID returns 404" do
        allow(http).to receive(:request).and_return(
          stub_http(
            status: 404,
            body: JSON.dump(
              error: {
                code: "0x80040217",
                message: "Record with id GUID does not exist",
              }
            )
          )
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {firstname: "John"},
            crm_id: "missing-guid"
          )
        end.to raise_error(Etlify::NotFound)
      end
    end

    context "with id_property and a value present in the payload" do
      let(:guid) { "11111111-2222-3333-4444-555555555555" }

      it "PATCHes by alternate key and returns the GUID from OData-EntityId" do
        expect(http).to receive(:request).with(
          :patch,
          entity_url("contacts", "emailaddress1='john@example.com'"),
          headers: hash_including("Authorization" => "Bearer fake-token"),
          body: satisfy do |body|
            JSON.parse(body)["firstname"] == "John"
          end
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "John", emailaddress1: "john@example.com"},
          id_property: "emailaddress1"
        )
        expect(result).to eq(guid)
      end

      it "accepts a Symbol id_property" do
        expect(http).to receive(:request).with(
          :patch,
          entity_url("contacts", "emailaddress1='john@example.com'"),
          anything
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {emailaddress1: "john@example.com"},
          id_property: :emailaddress1
        )
        expect(result).to eq(guid)
      end

      it "OData-escapes single quotes in the alternate key value" do
        expect(http).to receive(:request).with(
          :patch,
          entity_url("contacts", "emailaddress1='o''brian@example.com'"),
          anything
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        adapter.upsert!(
          object_type: "contacts",
          payload: {emailaddress1: "o'brian@example.com"},
          id_property: "emailaddress1"
        )
      end
    end

    context "with id_property but value missing from payload" do
      let(:guid) { "99999999-9999-9999-9999-999999999999" }

      it "falls back to POST create when the key is absent" do
        expect(http).to receive(:request).with(
          :post,
          "https://contoso.crm.dynamics.com/api/data/v9.2/contacts",
          anything
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "John"},
          id_property: "emailaddress1"
        )
        expect(result).to eq(guid)
      end

      it "falls back to POST create when the key value is an empty string" do
        expect(http).to receive(:request).with(
          :post,
          "https://contoso.crm.dynamics.com/api/data/v9.2/contacts",
          anything
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "John", emailaddress1: ""},
          id_property: "emailaddress1"
        )
        expect(result).to eq(guid)
      end

      it "falls back to POST create when the key value is nil" do
        expect(http).to receive(:request).with(
          :post,
          "https://contoso.crm.dynamics.com/api/data/v9.2/contacts",
          anything
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "John", emailaddress1: nil},
          id_property: "emailaddress1"
        )
        expect(result).to eq(guid)
      end
    end

    context "without crm_id and without id_property" do
      let(:guid) { "99999999-9999-9999-9999-999999999999" }

      it "POSTs create and returns the GUID from OData-EntityId" do
        expect(http).to receive(:request).with(
          :post,
          "https://contoso.crm.dynamics.com/api/data/v9.2/contacts",
          anything
        ).and_return(
          stub_http(status: 204, headers: entity_id_header("contacts", guid))
        )

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "John"}
        )
        expect(result).to eq(guid)
      end
    end

    describe "validations" do
      it "raises when object_type is blank" do
        expect do
          adapter.upsert!(object_type: "", payload: {})
        end.to raise_error(ArgumentError, /object_type/)
      end

      it "raises when payload is not a Hash" do
        expect do
          adapter.upsert!(object_type: "contacts", payload: "nope")
        end.to raise_error(ArgumentError, /payload/)
      end
    end

    describe "error mapping" do
      it "succeeds after a single 401 by transparently re-fetching the token" do
        call_count = 0
        allow(http).to receive(:request) do
          call_count += 1
          if call_count == 1
            stub_http(status: 401, body: "{}")
          else
            stub_http(
              status: 204,
              headers: entity_id_header("contacts", "abc")
            )
          end
        end

        result = adapter.upsert!(
          object_type: "contacts",
          payload: {emailaddress1: "x@y.z"},
          id_property: "emailaddress1"
        )
        expect(result).to eq("abc")
      end

      it "raises Unauthorized when 401 persists after retry" do
        allow(http).to receive(:request).and_return(
          stub_http(status: 401, body: "{}")
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {emailaddress1: "x@y.z"},
            id_property: "emailaddress1"
          )
        end.to raise_error(Etlify::Unauthorized)
      end

      it "raises ValidationFailed on 422" do
        allow(http).to receive(:request).and_return(
          stub_http(
            status: 422,
            body: JSON.dump(
              error: {
                code: "0x80040217",
                message: "Duplicate record detected",
              }
            )
          )
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {emailaddress1: "x@y.z"},
            id_property: "emailaddress1"
          )
        end.to raise_error(Etlify::ValidationFailed)
      end

      it "raises RateLimited on 429" do
        allow(http).to receive(:request).and_return(
          stub_http(status: 429, body: "{}")
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {emailaddress1: "x@y.z"},
            id_property: "emailaddress1"
          )
        end.to raise_error(Etlify::RateLimited)
      end

      it "raises ApiError when OData-EntityId is missing after upsert" do
        allow(http).to receive(:request).and_return(stub_http(status: 204))

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {emailaddress1: "x@y.z"},
            id_property: "emailaddress1"
          )
        end.to raise_error(Etlify::ApiError, /OData-EntityId/)
      end

      it "wraps transport errors into TransportError" do
        allow(http).to receive(:request).and_raise(
          StandardError, "ECONNRESET"
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {},
            crm_id: "abc"
          )
        end.to raise_error(Etlify::TransportError)
      end
    end
  end

  describe "#delete!" do
    it "returns true on 2xx" do
      expect(http).to receive(:request).with(
        :delete,
        "https://contoso.crm.dynamics.com/api/data/v9.2/contacts(abc-123)",
        headers: hash_including("Authorization" => "Bearer fake-token"),
        body: nil
      ).and_return(stub_http(status: 204))

      expect(
        adapter.delete!(object_type: "contacts", crm_id: "abc-123")
      ).to eq(true)
    end

    it "returns false on 404 (idempotent)" do
      allow(http).to receive(:request).and_return(stub_http(status: 404))

      expect(
        adapter.delete!(object_type: "contacts", crm_id: "missing")
      ).to eq(false)
    end

    it "raises on 401 after retry" do
      allow(http).to receive(:request).and_return(stub_http(status: 401))

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::Unauthorized)
    end

    it "raises ApiError on 500" do
      allow(http).to receive(:request).and_return(stub_http(status: 500))

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::ApiError)
    end

    it "raises ValidationFailed on 422" do
      allow(http).to receive(:request).and_return(
        stub_http(
          status: 422,
          body: JSON.dump(
            error: {
              code: "0x80048408",
              message: "Cannot delete record because of FK constraint",
            }
          )
        )
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::ValidationFailed)
    end

    it "raises RateLimited on 429" do
      allow(http).to receive(:request).and_return(stub_http(status: 429))

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::RateLimited)
    end

    it "wraps transport errors into TransportError" do
      allow(http).to receive(:request).and_raise(
        StandardError, "ECONNRESET"
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::TransportError)
    end

    it "raises on blank object_type" do
      expect do
        adapter.delete!(object_type: "", crm_id: "abc")
      end.to raise_error(ArgumentError, /object_type/)
    end

    it "raises on blank crm_id" do
      expect do
        adapter.delete!(object_type: "contacts", crm_id: "")
      end.to raise_error(ArgumentError, /crm_id/)
    end
  end

  describe "#batch_upsert!" do
    it "raises NotImplementedError with an explanatory message" do
      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          records: [{}],
          id_property: "emailaddress1"
        )
      end.to raise_error(NotImplementedError, /Batch operations/)
    end
  end

  describe "#batch_delete!" do
    it "raises NotImplementedError with an explanatory message" do
      expect do
        adapter.batch_delete!(
          object_type: "contacts",
          crm_ids: ["abc"]
        )
      end.to raise_error(NotImplementedError, /Batch operations/)
    end
  end

  describe "rate_limiter delegation" do
    it "exposes a rate_limiter accessor that delegates to the client" do
      limiter = double("limiter")
      adapter.rate_limiter = limiter
      expect(adapter.rate_limiter).to be(limiter)
    end

    it "returns nil when no rate_limiter has been assigned" do
      expect(adapter.rate_limiter).to be_nil
    end
  end
end
