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
        :post,
        "https://api.intercom.io/contacts",
        headers: hash_including(
          "Authorization" => "Bearer #{token}",
          "Intercom-Version" => "2.14"
        ),
        body: kind_of(String)
      ).and_return({status: 200, body: {id: "abc"}.to_json})

      adapter.upsert!(object_type: "contacts", payload: {email: "a@b.com"})
    end

    it "uses the EU base URL when region: :eu" do
      eu_adapter = described_class.new(
        access_token: token, region: :eu, http_client: http
      )

      expect(http).to receive(:request).with(
        :post,
        "https://api.eu.intercom.io/contacts",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: kind_of(String)
      ).and_return({status: 200, body: {id: "abc"}.to_json})

      eu_adapter.upsert!(object_type: "contacts", payload: {email: "a@b.com"})
    end

    it "uses the AU base URL when region: :au" do
      au_adapter = described_class.new(
        access_token: token, region: :au, http_client: http
      )

      expect(http).to receive(:request).with(
        :post,
        "https://api.au.intercom.io/contacts",
        anything
      ).and_return({status: 200, body: {id: "abc"}.to_json})

      au_adapter.upsert!(object_type: "contacts", payload: {email: "a@b.com"})
    end

    it "honors a custom api_version" do
      custom_adapter = described_class.new(
        access_token: token, api_version: "2.11", http_client: http
      )

      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts",
        headers: hash_including("Intercom-Version" => "2.11"),
        body: kind_of(String)
      ).and_return({status: 200, body: {id: "abc"}.to_json})

      custom_adapter.upsert!(
        object_type: "contacts", payload: {email: "a@b.com"}
      )
    end

    it "raises on an unknown region" do
      expect do
        described_class.new(access_token: token, region: :asia)
      end.to raise_error(ArgumentError, /region must be one of/)
    end
  end

  describe "#upsert!" do
    context "when object exists (search by id_property)" do
      it "PUTs the object and returns its id", :aggregate_failures do
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

        expect(http).to receive(:request).with(
          :put,
          "https://api.intercom.io/contacts/abc123",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json == {
              "external_id" => "u_1",
              "email" => "john@example.com",
              "name" => "John",
            }
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {external_id: "u_1", email: "john@example.com", name: "John"},
          id_property: "external_id"
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
          payload: {email: "john@example.com", name: "John"},
          id_property: "external_id",
          crm_id: "abc123"
        )
        expect(id).to eq("abc123")
      end
    end

    context "when object does not exist yet" do
      it "POSTs a new object and returns its id", :aggregate_failures do
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
            json["external_id"] == "u_1" &&
              json["email"] == "john@example.com"
          end
        ).and_return({status: 200, body: {id: "abc999"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {external_id: "u_1", email: "john@example.com"},
          id_property: "external_id"
        )
        expect(id).to eq("abc999")
      end
    end

    context "when no id_property is provided" do
      it "creates directly and returns the new id", :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.intercom.io/companies",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            JSON.parse(body) == {"name" => "ACME"}
          end
        ).and_return({status: 200, body: {id: "co_1"}.to_json})

        id = adapter.upsert!(
          object_type: "companies", payload: {name: "ACME"}
        )
        expect(id).to eq("co_1")
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
          payload: {company_id: "ext_42", name: "ACME"},
          id_property: "company_id"
        )
        expect(id).to eq("co_42")
      end
    end

    it "lowercases email for contacts on search and create",
       :aggregate_failures do
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
        id_property: "email"
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
        id_property: "email"
      )
      expect(id).to eq("abc")
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.upsert!(object_type: "", payload: {})
      end.to raise_error(ArgumentError)

      expect do
        adapter.upsert!(object_type: "contacts", payload: "not a hash")
      end.to raise_error(ArgumentError)
    end

    it "creates directly when id_property is missing from payload" do
      expect(http).not_to receive(:request).with(
        :post, %r{/contacts/search}, anything
      )

      expect(http).to receive(:request).with(
        :post,
        "https://api.intercom.io/contacts",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |body| JSON.parse(body)["name"] == "J" }
      ).and_return({status: 200, body: {id: "abc"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts", payload: {name: "J"}, id_property: "email"
      )
      expect(id).to eq("abc")
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
        payload: {email: "x@y.com"},
        id_property: "email"
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
        payload: {email: "x@y.com"},
        id_property: "email"
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
          payload: {email: "x@y.com"},
          id_property: "email"
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
          payload: {email: "a@b.com"},
          id_property: "email"
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
          payload: {email: "x@y.com"},
          id_property: "email"
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
          payload: {email: "x@y.com"},
          id_property: "email"
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
          payload: {email: "x@y.com"},
          id_property: "email"
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
          payload: {email: "x@y.com"},
          id_property: "email"
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
            payload: {email: "x@y.com"},
            id_property: "email"
          )
        end.to raise_error(
          Etlify::TransportError, /HTTP transport error: StandardError: boom/
        )
      end
    end

    it "sends standard JSON headers including Intercom-Version on create",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, %r{/companies},
        headers: include(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
          "Intercom-Version" => "2.14"
        ),
        body: kind_of(String)
      ).and_return({status: 200, body: {id: "co_1"}.to_json})

      id = adapter.upsert!(
        object_type: "companies", payload: {name: "ACME"}
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
    it "loops sequentially over upsert! and aggregates the mapping",
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
        records: [
          {external_id: "u_1", email: "john@example.com"},
          {external_id: "u_2", email: "jane@example.com"},
        ],
        id_property: "external_id"
      )

      expect(result).to eq("u_1" => "id_1", "u_2" => "id_2")
    end

    it "skips records without an id_property value", :aggregate_failures do
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
        records: [
          {external_id: "u_1", email: "a@b.com"},
          {email: "no-id@example.com"},
        ],
        id_property: "external_id"
      )

      expect(result).to eq("u_1" => "id_1")
    end

    it "raises ArgumentError on invalid arguments", :aggregate_failures do
      expect do
        adapter.batch_upsert!(
          object_type: "",
          records: [{external_id: "u_1"}],
          id_property: "external_id"
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          records: [{external_id: "u_1"}],
          id_property: nil
        )
      end.to raise_error(ArgumentError, /id_property/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          records: "not array",
          id_property: "external_id"
        )
      end.to raise_error(ArgumentError, /records/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          records: [],
          id_property: "external_id"
        )
      end.to raise_error(ArgumentError, /records/)
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
          records: [{external_id: "u_1", email: "a@b.com"}],
          id_property: "external_id"
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
