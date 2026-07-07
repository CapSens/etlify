require "rails_helper"
require "etlify/adapters/intercom_adapter"

RSpec.describe Etlify::Adapters::IntercomAdapter do
  let(:token) { "test-token" }
  let(:http)  { instance_double("HttpClient") }

  subject(:adapter) do
    described_class.new(access_token: token, http_client: http)
  end

  describe "#initialize" do
    it "defaults to the US region and Intercom-Version 2.14" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.intercom.io/contacts/abc",
        headers: hash_including(
          "Authorization" => "Bearer #{token}",
          "Intercom-Version" => "2.14"
        ),
        body: nil
      ).and_return({status: 200, body: "{}"})

      adapter.delete!(object_type: "contacts", crm_id: "abc")
    end

    it "uses the EU base URL when region: :eu" do
      eu_adapter = described_class.new(
        access_token: token, region: :eu, http_client: http
      )

      expect(http).to receive(:request).with(
        :delete,
        "https://api.eu.intercom.io/contacts/abc",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 200, body: "{}"})

      eu_adapter.delete!(object_type: "contacts", crm_id: "abc")
    end

    it "uses the AU base URL when region: :au" do
      au_adapter = described_class.new(
        access_token: token, region: :au, http_client: http
      )

      expect(http).to receive(:request).with(
        :delete,
        "https://api.au.intercom.io/contacts/abc",
        headers: anything,
        body: nil
      ).and_return({status: 200, body: "{}"})

      au_adapter.delete!(object_type: "contacts", crm_id: "abc")
    end

    it "honors a custom api_version" do
      custom_adapter = described_class.new(
        access_token: token, api_version: "2.11", http_client: http
      )

      expect(http).to receive(:request).with(
        :delete,
        "https://api.intercom.io/contacts/abc",
        headers: hash_including("Intercom-Version" => "2.11"),
        body: nil
      ).and_return({status: 200, body: "{}"})

      custom_adapter.delete!(object_type: "contacts", crm_id: "abc")
    end

    it "raises on an unknown region" do
      expect do
        described_class.new(access_token: token, region: :asia)
      end.to raise_error(ArgumentError, /region must be one of/)
    end
  end

  describe "#upsert!" do
    context "when object exists (search by match_property)" do
      it "PUTs the payload as-is and returns its id", :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.intercom.io/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["query"] == {
              "field" => "external_id",
              "operator" => "=",
              "value" => "u_1",
            }
          end
        ).and_return(
          {status: 200, body: {data: [{"id" => "abc123"}]}.to_json}
        )

        # The matching property is never written on an existing object
        # unless the payload explicitly includes it.
        expect(http).to receive(:request).with(
          :put,
          "https://api.intercom.io/contacts/abc123",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            JSON.parse(body) == {"name" => "John"}
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {name: "John"},
          match_property: "external_id",
          match_value: "u_1"
        )
        expect(id).to eq("abc123")
      end
    end

    context "when crm_id is provided" do
      it "skips search and PUTs directly", :aggregate_failures do
        expect(http).not_to receive(:request).with(
          :post,
          %r{/contacts/search},
          anything
        )

        expect(http).to receive(:request).with(
          :put,
          "https://api.intercom.io/contacts/abc123",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            JSON.parse(body)["name"] == "John"
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {name: "John"},
          match_property: "external_id",
          match_value: "u_1",
          crm_id: "abc123"
        )
        expect(id).to eq("abc123")
      end

      it "accepts a blank match_value when crm_id is known" do
        expect(http).to receive(:request).with(
          :put,
          "https://api.intercom.io/contacts/abc123",
          headers: anything,
          body: kind_of(String)
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {name: "John"},
          match_property: "external_id",
          match_value: nil,
          crm_id: "abc123"
        )
        expect(id).to eq("abc123")
      end
    end

    context "when object does not exist yet" do
      it "POSTs a new object with the match property and returns its id",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.intercom.io/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 200, body: {data: []}.to_json})

        # The matching property is written at creation time when the
        # payload does not already carry it.
        expect(http).to receive(:request).with(
          :post,
          "https://api.intercom.io/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["external_id"] == "u_1" &&
              json["email"] == "john@example.com"
          end
        ).and_return({status: 200, body: {id: "abc999"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "john@example.com"},
          match_property: "external_id",
          match_value: "u_1"
        )
        expect(id).to eq("abc999")
      end

      it "lets the payload win over match_value at creation time",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          %r{/contacts/search},
          anything
        ).and_return({status: 200, body: {data: []}.to_json})

        expect(http).to receive(:request).with(
          :post,
          "https://api.intercom.io/contacts",
          headers: anything,
          body: satisfy do |body|
            JSON.parse(body)["external_id"] == "custom"
          end
        ).and_return({status: 200, body: {id: "abc999"}.to_json})

        adapter.upsert!(
          object_type: "contacts",
          payload: {external_id: "custom"},
          match_property: "external_id",
          match_value: "u_1"
        )
      end
    end

    context "with the companies resource" do
      it "searches via /companies/search then PUTs", :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.intercom.io/companies/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            JSON.parse(body)["query"] == {
              "field" => "company_id",
              "operator" => "=",
              "value" => "ext_42",
            }
          end
        ).and_return(
          {status: 200, body: {data: [{"id" => "co_42"}]}.to_json}
        )

        expect(http).to receive(:request).with(
          :put,
          "https://api.intercom.io/companies/co_42",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            JSON.parse(body)["name"] == "ACME"
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "companies",
          payload: {name: "ACME"},
          match_property: "company_id",
          match_value: "ext_42"
        )
        expect(id).to eq("co_42")
      end
    end

    it "lowercases the email match_value on search and the payload email " \
       "on create for contacts", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy do |body|
          JSON.parse(body)["query"]["value"] == "john@example.com"
        end
      ).and_return({status: 200, body: {data: []}.to_json})

      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy do |body|
          JSON.parse(body)["email"] == "john@example.com"
        end
      ).and_return({status: 200, body: {id: "abc"}.to_json})

      adapter.upsert!(
        object_type: "contacts",
        payload: {email: "John@Example.COM"},
        match_property: "email",
        match_value: "John@Example.COM"
      )
    end

    it "accepts string or symbol keys in payload", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: kind_of(String)
      ).and_return({status: 200, body: {data: []}.to_json})

      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy do |body|
          json = JSON.parse(body)
          json["email"] == "a@b.com" && json["name"] == "A"
        end
      ).and_return({status: 200, body: {id: "abc"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {"email" => "a@b.com", :name => "A"},
        match_property: "email",
        match_value: "a@b.com"
      )
      expect(id).to eq("abc")
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.upsert!(
          object_type: "",
          payload: {},
          match_property: "email",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: "not a hash",
          match_property: "email",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError, /payload/)

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError, /match_property/)
    end

    it "raises ArgumentError when match_value is blank and crm_id unknown" do
      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {name: "J"},
          match_property: "email",
          match_value: "  "
        )
      end.to raise_error(ArgumentError, /match_value/)
    end

    it "treats malformed 200 search payload as not found then creates",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        anything
      ).and_return({status: 200, body: {}.to_json})

      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts",
        anything
      ).and_return({status: 200, body: {id: "new"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {},
        match_property: "email",
        match_value: "x@y.com"
      )
      expect(id).to eq("new")
    end

    it "treats search 404 as not found and creates", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        anything
      ).and_return({status: 404, body: ""})

      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts",
        anything
      ).and_return({status: 200, body: {id: "new"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {},
        match_property: "email",
        match_value: "x@y.com"
      )
      expect(id).to eq("new")
    end

    it "raises ValidationFailed on 422 update", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/contacts/search}, anything
      ).and_return(
        {status: 200, body: {data: [{"id" => "abc"}]}.to_json}
      )

      expect(http).to receive(:request).with(
        :put, "https://api.intercom.io/contacts/abc", anything
      ).and_return(
        {
          status: 422,
          body: {
            type: "error.list",
            errors: [
              {
                code: "parameter_invalid",
                message: "Email is invalid",
                field: "email",
              },
            ],
          }.to_json,
        }
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "email",
          match_value: "x@y.com"
        )
      end.to raise_error(Etlify::ValidationFailed, /Email is invalid/)
    end

    it "raises RateLimited on 429 create", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/contacts/search}, anything
      ).and_return({status: 200, body: {data: []}.to_json})

      expect(http).to receive(:request).with(
        :post, "https://api.intercom.io/contacts", anything
      ).and_return(
        {
          status: 429,
          body: {
            type: "error.list",
            errors: [{code: "rate_limit", message: "Too many requests"}],
          }.to_json,
        }
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "email",
          match_value: "a@b.com"
        )
      end.to raise_error(Etlify::RateLimited, /Too many requests/)
    end

    it "raises Unauthorized on 401 search", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/contacts/search}, anything
      ).and_return(
        {
          status: 401,
          body: {
            type: "error.list",
            errors: [{code: "token_unauthorized", message: "Bad token"}],
          }.to_json,
        }
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "email",
          match_value: "x@y.com"
        )
      end.to raise_error(Etlify::Unauthorized, /Bad token/)
    end

    it "raises Unauthorized on 403 search", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/contacts/search}, anything
      ).and_return(
        {
          status: 403,
          body: {
            type: "error.list",
            errors: [{code: "forbidden", message: "Nope"}],
          }.to_json,
        }
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "email",
          match_value: "x@y.com"
        )
      end.to raise_error(Etlify::Unauthorized, /Nope/)
    end

    it "raises ApiError with generic message when body is non-JSON",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/contacts/search}, anything
      ).and_return({status: 500, body: "<html>oops</html>"})

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "email",
          match_value: "x@y.com"
        )
      end.to raise_error(Etlify::ApiError, /Intercom API request failed/)
    end

    it "wraps transport errors during update into TransportError",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/contacts/search}, anything
      ).and_return(
        {status: 200, body: {data: [{"id" => "abc"}]}.to_json}
      )

      expect(http).to receive(:request).with(
        :put, %r{/contacts/abc}, anything
      ).and_raise(StandardError.new("tcp reset"))

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "email",
          match_value: "x@y.com"
        )
      end.to raise_error(Etlify::TransportError, /tcp reset/)
    end

    context "when transport layer fails" do
      it "wraps the error into TransportError" do
        expect(http).to receive(:request).and_raise(
          StandardError.new("boom")
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {},
            match_property: "email",
            match_value: "x@y.com"
          )
        end.to raise_error(
          Etlify::TransportError, /HTTP transport error: StandardError: boom/
        )
      end
    end

    it "sends standard JSON headers including Intercom-Version on create",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/companies/search}, anything
      ).and_return({status: 200, body: {data: []}.to_json})

      expect(http).to receive(:request).with(
        :post, "https://api.intercom.io/companies",
        headers: include(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
          "Intercom-Version" => "2.14"
        ),
        body: kind_of(String)
      ).and_return({status: 200, body: {id: "co_1"}.to_json})

      id = adapter.upsert!(
        object_type: "companies",
        payload: {name: "ACME"},
        match_property: "company_id",
        match_value: "ext_1"
      )
      expect(id).to eq("co_1")
    end
  end

  describe "#delete!" do
    it "returns true on 2xx response", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.intercom.io/contacts/abc",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 200, body: "{}"})

      expect(adapter.delete!(object_type: "contacts", crm_id: "abc")).to be true
    end

    it "returns false on 404", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.intercom.io/contacts/abc",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 404, body: ""})

      expect(adapter.delete!(object_type: "contacts", crm_id: "abc")).to be false
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.delete!(object_type: "", crm_id: "abc")
      end.to raise_error(ArgumentError)

      expect do
        adapter.delete!(object_type: "contacts", crm_id: nil)
      end.to raise_error(ArgumentError)

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "")
      end.to raise_error(ArgumentError)
    end

    it "raises ApiError on 400" do
      expect(http).to receive(:request).with(
        :delete, "https://api.intercom.io/contacts/abc",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return(
        {
          status: 400,
          body: {
            type: "error.list",
            errors: [{code: "bad_request", message: "Bad"}],
          }.to_json,
        }
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::ApiError, /Bad/)
    end

    it "raises Unauthorized on 401" do
      expect(http).to receive(:request).and_return(
        {
          status: 401,
          body: {
            type: "error.list",
            errors: [{code: "token_unauthorized", message: "Bad token"}],
          }.to_json,
        }
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::Unauthorized, /Bad token/)
    end

    it "raises ApiError on 500" do
      expect(http).to receive(:request).and_return(
        {
          status: 500,
          body: {
            type: "error.list",
            errors: [{code: "server_error", message: "Boom"}],
          }.to_json,
        }
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::ApiError, /Boom/)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(
        StandardError.new("network oops")
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "abc")
      end.to raise_error(Etlify::TransportError, /network oops/)
    end
  end

  describe "#batch_upsert!" do
    it "loops sequentially over upsert! and maps values as provided",
       :aggregate_failures do
      # John: search hit + update
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        headers: anything,
        body: satisfy { |b| JSON.parse(b)["query"]["value"] == "u_1" }
      ).and_return(
        {status: 200, body: {data: [{"id" => "id_1"}]}.to_json}
      )
      expect(http).to receive(:request).with(
        :put, "https://api.intercom.io/contacts/id_1", anything
      ).and_return({status: 200, body: "{}"})

      # Jane: search miss + create
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        headers: anything,
        body: satisfy { |b| JSON.parse(b)["query"]["value"] == "u_2" }
      ).and_return({status: 200, body: {data: []}.to_json})
      expect(http).to receive(:request).with(
        :post, "https://api.intercom.io/contacts", anything
      ).and_return({status: 200, body: {id: "id_2"}.to_json})

      result = adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [
          {value: "u_1", properties: {email: "john@example.com"}},
          {value: "u_2", properties: {email: "jane@example.com"}},
        ],
        match_property: "external_id"
      )

      expect(result).to eq("u_1" => "id_1", "u_2" => "id_2")
    end

    it "accepts string keys in inputs", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        headers: anything,
        body: satisfy { |b| JSON.parse(b)["query"]["value"] == "u_1" }
      ).and_return({status: 200, body: {data: []}.to_json})
      expect(http).to receive(:request).with(
        :post, "https://api.intercom.io/contacts", anything
      ).and_return({status: 200, body: {id: "id_1"}.to_json})

      result = adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [{"value" => "u_1", "properties" => {email: "a@b.com"}}],
        match_property: "external_id"
      )

      expect(result).to eq("u_1" => "id_1")
    end

    it "keeps the mapping keyed by the value as provided when the CRM " \
       "normalizes it", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts/search",
        headers: anything,
        body: satisfy do |b|
          JSON.parse(b)["query"]["value"] == "john@example.com"
        end
      ).and_return({status: 200, body: {data: []}.to_json})
      expect(http).to receive(:request).with(
        :post, "https://api.intercom.io/contacts", anything
      ).and_return({status: 200, body: {id: "id_1"}.to_json})

      result = adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [{value: "John@Example.COM", properties: {}}],
        match_property: "email"
      )

      expect(result).to eq("John@Example.COM" => "id_1")
    end

    it "raises ArgumentError when an input carries a blank value" do
      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "", properties: {email: "a@b.com"}}],
          match_property: "external_id"
        )
      end.to raise_error(ArgumentError, /non-blank :value/)
    end

    it "raises ArgumentError on invalid arguments", :aggregate_failures do
      expect do
        adapter.batch_upsert!(
          object_type: "",
          inputs: [{value: "u_1", properties: {}}],
          match_property: "external_id"
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "u_1", properties: {}}],
          match_property: nil
        )
      end.to raise_error(ArgumentError, /match_property/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: "not array",
          match_property: "external_id"
        )
      end.to raise_error(ArgumentError, /inputs/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [],
          match_property: "external_id"
        )
      end.to raise_error(ArgumentError, /inputs/)
    end

    it "propagates RateLimited from the underlying upsert!" do
      expect(http).to receive(:request).with(
        :post, %r{/contacts/search}, anything
      ).and_return(
        {
          status: 429,
          body: {
            type: "error.list",
            errors: [{code: "rate_limit", message: "Too many"}],
          }.to_json,
        }
      )

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "u_1", properties: {email: "a@b.com"}}],
          match_property: "external_id"
        )
      end.to raise_error(Etlify::RateLimited, /Too many/)
    end
  end

  describe "#batch_update!" do
    it "PUTs each record by crm_id and returns an identity mapping",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :put,
        "https://api.intercom.io/contacts/id_1",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |b| JSON.parse(b) == {"name" => "John"} }
      ).and_return({status: 200, body: "{}"})

      expect(http).to receive(:request).with(
        :put,
        "https://api.intercom.io/contacts/id_2",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |b| JSON.parse(b) == {"name" => "Jane"} }
      ).and_return({status: 200, body: "{}"})

      result = adapter.batch_update!(
        object_type: "contacts",
        records: [
          {crm_id: "id_1", properties: {name: "John"}},
          {crm_id: "id_2", properties: {name: "Jane"}},
        ]
      )

      expect(result).to eq("id_1" => "id_1", "id_2" => "id_2")
    end

    it "accepts string keys and lowercases contact emails",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :put,
        "https://api.intercom.io/contacts/id_1",
        headers: anything,
        body: satisfy { |b| JSON.parse(b)["email"] == "john@example.com" }
      ).and_return({status: 200, body: "{}"})

      result = adapter.batch_update!(
        object_type: "contacts",
        records: [
          {"crm_id" => "id_1", "properties" => {email: "John@Example.COM"}},
        ]
      )

      expect(result).to eq("id_1" => "id_1")
    end

    it "raises ArgumentError on invalid arguments", :aggregate_failures do
      expect do
        adapter.batch_update!(
          object_type: "",
          records: [{crm_id: "id_1", properties: {}}]
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_update!(object_type: "contacts", records: "not array")
      end.to raise_error(ArgumentError, /records/)

      expect do
        adapter.batch_update!(object_type: "contacts", records: [])
      end.to raise_error(ArgumentError, /records/)

      expect do
        adapter.batch_update!(
          object_type: "contacts",
          records: [{crm_id: "", properties: {}}]
        )
      end.to raise_error(ArgumentError, /crm_id/)
    end

    it "raises NotFound when a crm_id points to a deleted object" do
      expect(http).to receive(:request).with(
        :put, "https://api.intercom.io/contacts/gone", anything
      ).and_return(
        {
          status: 404,
          body: {
            type: "error.list",
            errors: [{code: "not_found", message: "Contact Not Found"}],
          }.to_json,
        }
      )

      expect do
        adapter.batch_update!(
          object_type: "contacts",
          records: [{crm_id: "gone", properties: {name: "J"}}]
        )
      end.to raise_error(Etlify::NotFound, /Contact Not Found/)
    end

    it "propagates RateLimited from the underlying update" do
      expect(http).to receive(:request).with(
        :put, %r{/contacts/id_1}, anything
      ).and_return(
        {
          status: 429,
          body: {
            type: "error.list",
            errors: [{code: "rate_limit", message: "Too many"}],
          }.to_json,
        }
      )

      expect do
        adapter.batch_update!(
          object_type: "contacts",
          records: [{crm_id: "id_1", properties: {name: "J"}}]
        )
      end.to raise_error(Etlify::RateLimited, /Too many/)
    end
  end

  describe "#batch_delete!" do
    it "calls delete! sequentially and returns true", :aggregate_failures do
      ["abc", "def", "ghi"].each do |id|
        expect(http).to receive(:request).with(
          :delete,
          "https://api.intercom.io/contacts/#{id}",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: nil
        ).and_return({status: 200, body: "{}"})
      end

      expect(
        adapter.batch_delete!(
          object_type: "contacts", crm_ids: ["abc", "def", "ghi"]
        )
      ).to be true
    end

    it "ignores 404 (treated as already deleted)", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete, "https://api.intercom.io/contacts/abc",
        headers: anything, body: nil
      ).and_return({status: 200, body: "{}"})
      expect(http).to receive(:request).with(
        :delete, "https://api.intercom.io/contacts/def",
        headers: anything, body: nil
      ).and_return({status: 404, body: ""})

      expect(
        adapter.batch_delete!(
          object_type: "contacts", crm_ids: ["abc", "def"]
        )
      ).to be true
    end

    it "raises ArgumentError on invalid arguments", :aggregate_failures do
      expect do
        adapter.batch_delete!(object_type: "", crm_ids: ["abc"])
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_delete!(object_type: "contacts", crm_ids: "not array")
      end.to raise_error(ArgumentError, /crm_ids/)

      expect do
        adapter.batch_delete!(object_type: "contacts", crm_ids: [])
      end.to raise_error(ArgumentError, /crm_ids/)
    end

    it "raises on the first non-404 error" do
      expect(http).to receive(:request).with(
        :delete, "https://api.intercom.io/contacts/abc",
        headers: anything, body: nil
      ).and_return(
        {
          status: 401,
          body: {
            type: "error.list",
            errors: [{code: "token_unauthorized", message: "Bad token"}],
          }.to_json,
        }
      )

      expect do
        adapter.batch_delete!(
          object_type: "contacts", crm_ids: ["abc", "def"]
        )
      end.to raise_error(Etlify::Unauthorized, /Bad token/)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(
        StandardError.new("dns failure")
      )

      expect do
        adapter.batch_delete!(
          object_type: "contacts", crm_ids: ["abc"]
        )
      end.to raise_error(Etlify::TransportError, /dns failure/)
    end
  end
end
