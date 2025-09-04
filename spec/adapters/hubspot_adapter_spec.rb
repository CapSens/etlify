# frozen_string_literal: true

require "rails_helper"
require "etlify/adapters/hubspot_v3_adapter"

RSpec.describe Etlify::Adapters::HubspotV3Adapter do
  let(:token) { "test-token" }
  let(:http)  { instance_double("HttpClient") }

  subject(:adapter) do
    described_class.new(access_token: token, http_client: http)
  end

  describe "#upsert!" do
    context "when object exists (search by id_property) for native type" do
      it "PATCHes the object and returns its id", :aggregate_failures do
        # 1) Search
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["filterGroups"].first["filters"].first["propertyName"] == "email" &&
              json["filterGroups"].first["filters"].first["value"] ==
              "john@example.com"
          end
        ).and_return(
          {status: 200, body: {results: [{"id" => "1234"}]}.to_json}
        )

        # 2) Update
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] == {"firstname" => "John"}
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "john@example.com", firstname: "John"},
          id_property: "email"
        )
        expect(id).to eq("1234")
      end
    end

    context "when crm_id is provided" do
      it "skips search and PATCHes directly, returning the id",
         :aggregate_failures do
        # Must NOT hit the /search endpoint
        expect(http).not_to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          anything
        )

        # Direct update on the provided crm_id
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            props = json["properties"]
            # We only assert what's essential for this scenario
            props.is_a?(Hash) && props["firstname"] == "John"
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "ignored@example.com", firstname: "John"},
          id_property: "email",
          crm_id: "1234"
        )

        expect(id).to eq("1234")
      end
    end

    context "when object does not exist yet (native type)" do
      it "POSTs a new object and returns its id", :aggregate_failures do
        # 1) Search → no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 200, body: {results: []}.to_json})

        # 2) Create
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] == {"firstname" => "John", "email" => "john@example.com"}
          end
        ).and_return({status: 201, body: {id: "5678"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "john@example.com", firstname: "John"},
          id_property: "email"
        )
        expect(id).to eq("5678")
      end
    end

    context "when no id_property is provided (e.g., deals)" do
      it "creates directly and returns the new id", :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/deals",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] == {"dealname" => "New deal", "amount" => 1000}
          end
        ).and_return({status: 201, body: {id: "9999"}.to_json})

        id = adapter.upsert!(
          object_type: "deals",
          payload: {dealname: "New deal", amount: 1000}
        )
        expect(id).to eq("9999")
      end
    end

    context "with custom object type" do
      it "searches and creates/updates using the provided custom type", :aggregate_failures do
        custom_type = "p12345_myobject"

        # 1) Search → no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/#{custom_type}/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 200, body: {results: []}.to_json})

        # 2) Create
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/#{custom_type}",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] == {"unique_code" => "ABC-001", "name" => "Custom A"}
          end
        ).and_return({status: 201, body: {id: "42"}.to_json})

        id = adapter.upsert!(
          object_type: custom_type,
          payload: {unique_code: "ABC-001", name: "Custom A"},
          id_property: "unique_code"
        )
        expect(id).to eq("42")
      end
    end

    context "email matching quirks" do
      it "searches with lowercased email and handles '+' primary", :aggregate_failures do
        email_in  = "John+Stage@Example.com"
        email_lc  = "john+stage@example.com"
        email_enc = "john%2Bstage@example.com"

        # 1) Search should include:
        #   - EQ on email (lowercased)
        #   - CONTAINS_TOKEN on hs_additional_emails (lowercased)
        #   - EQ on email with %2B fallback
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            groups = json["filterGroups"]
            # sanity checks
            expect(groups).to be_an(Array)
            expect(groups.size).to be >= 2

            eq_email = groups.any? do |group|
              filter = group["filters"].first
              expected_filter = {
                "propertyName" => "email",
                "operator" => "EQ",
                "value" => email_lc,
              }
              expected_filter == filter
            end

            contains_token = groups.any? do |group|
              filter = group["filters"].first
              expected_filter = {
                "propertyName" => "hs_additional_emails",
                "operator" => "CONTAINS_TOKEN",
                "value" => email_lc,
              }
              expected_filter == filter
            end

            eq_email_fallback = groups.any? do |group|
              filter = group["filters"].first
              expected_filter = {
                "propertyName" => "email",
                "operator" => "EQ",
                "value" => email_enc,
              }
              expected_filter == filter
            end

            eq_email && contains_token && eq_email_fallback
          end
        ).and_return(
          {status: 200, body: {results: [{"id" => "222"}]}.to_json}
        )

        # 2) Update
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/222",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            j = JSON.parse(body)
            j["properties"] == {"firstname" => "John"}
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: email_in, firstname: "John"},
          id_property: "email"
        )
        expect(id).to eq("222")
      end

      it "finds by hs_additional_emails when primary differs", :aggregate_failures do
        # 1) Search should include CONTAINS_TOKEN on hs_additional_emails
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            groups = json["filterGroups"]
            expect(groups).to be_an(Array)
            groups.any? do |group|
              filter = group["filters"].first
              expected_filter = {
                "propertyName" => "hs_additional_emails",
                "operator" => "CONTAINS_TOKEN",
                "value" => "alias+promo@example.com",
              }
              expected_filter == filter
            end
          end
        ).and_return(
          {status: 200, body: {results: [{"id" => "333"}]}.to_json}
        )

        # 2) Update
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/333",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] == {"firstname" => "A"}
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "Alias+Promo@Example.com", firstname: "A"},
          id_property: "email"
        )
        expect(id).to eq("333")
      end

      it "creates when '+' email not found (still lowercases on create)", :aggregate_failures do
        # 1) Search → no result (200 empty array)
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 200, body: {results: []}.to_json})

        # 2) Create: email should be lowercased in properties
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] ==
              {"firstname" => "J", "email" => "john+tag@example.com"}
          end
        ).and_return({status: 201, body: {id: "444"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "John+Tag@Example.com", firstname: "J"},
          id_property: "email"
        )
        expect(id).to eq("444")
      end

      it "lowercases email for search even without '+'", :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            expected_filter = {
              "propertyName" => "email",
              "operator" => "EQ",
              "value" => "john@example.com",
            }
            filter = json["filterGroups"].first["filters"].first
            expected_filter == filter
          end
        ).and_return({status: 200, body: {results: [{"id" => "555"}]}.to_json})

        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/555",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "John@Example.com", firstname: "J"},
          id_property: "email"
        )
        expect(id).to eq("555")
      end
    end

    it "accepts string or symbol keys in payload", :aggregate_failures do
      # Search → no results
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: kind_of(String)
      ).and_return({status: 200, body: {results: []}.to_json})

      # Create includes both properties
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy do |body|
          json = JSON.parse(body)
          json["properties"] == {"email" => "a@b.com", "firstname" => "A"}
        end
      ).and_return({status: 201, body: {id: "314"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {"email" => "a@b.com", :firstname => "A"},
        id_property: "email"
      )
      expect(id).to eq("314")
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.upsert!(object_type: "", payload: {})
      end.to raise_error(ArgumentError)

      expect do
        adapter.upsert!(object_type: "contacts", payload: "not a hash")
      end.to raise_error(ArgumentError)
    end

    it "creates directly when id_property is missing in payload" do
      expect(http).not_to receive(:request).with(:post, /\/search/, anything)

      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts",
        headers: hash_including(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json"
        ),
        body: satisfy { |b| JSON.parse(b)["properties"] == {"firstname" => "J"} }
      ).and_return({status: 201, body: {id: "1001"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts", payload: {firstname: "J"}, id_property: "email"
      )
      expect(id).to eq("1001")
    end

    it "treats malformed 200 search payload as not found then creates", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts/search", anything
      ).and_return({status: 200, body: {}.to_json})

      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts", anything
      ).and_return({status: 201, body: {id: "1002"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {email: "x@y.com", firstname: "X"},
        id_property: "email"
      )
      expect(id).to eq("1002")
    end

    it "falls back to create when search result has no id", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts/search", anything
      ).and_return({status: 200, body: {results: [{}]}.to_json})

      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts", anything
      ).and_return({status: 201, body: {id: "1003"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {email: "x@y.com", firstname: "X"},
        id_property: "email"
      )
      expect(id).to eq("1003")
    end

    it "PATCHes and succeeds with 204", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({status: 200, body: {results: [{"id" => "55"}]}.to_json})

      expect(http).to receive(:request).with(
        :patch, "https://api.hubapi.com/crm/v3/objects/contacts/55", anything
      ).and_return({status: 204, body: ""})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {email: "john@example.com", firstname: "John"},
        id_property: "email"
      )
      expect(id).to eq("55")
    end

    it "raises ValidationFailed on 422 update", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({status: 200, body: {results: [{"id" => "9"}]}.to_json})

      expect(http).to receive(:request).with(
        :patch, "https://api.hubapi.com/crm/v3/objects/contacts/9", anything
      ).and_return(
        {status: 422, body: {message: "Invalid", category: "X"}.to_json}
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {email: "john@example.com"},
          id_property: "email"
        )
      end.to raise_error(Etlify::ValidationFailed, /Invalid/)
    end

    it "raises RateLimited on 429 create", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({status: 200, body: {results: []}.to_json})

      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/contacts", anything
      ).and_return(
        {
          status: 429,
          body: {message: "RL", category: "RATE_LIMITS"}.to_json,
        }
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {email: "a@b.com"},
          id_property: "email"
        )
      end.to raise_error(Etlify::RateLimited, /RL/)
    end

    it "raises Unauthorized on 403 search", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({status: 403, body: {message: "Forbidden"}.to_json})

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {email: "x@y.com"},
          id_property: "email"
        )
      end.to raise_error(Etlify::Unauthorized, /Forbidden/)
    end

    it "extracts unique value whether key is string or symbol", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy do |body|
          json = JSON.parse(body)
          filter = json["filterGroups"].first["filters"].first
          expected_filter = {
            "propertyName" => "email",
            "operator" => "EQ",
            "value" => "s@y.com",
          }
          filter == expected_filter
        end
      ).and_return({status: 200, body: {results: [{"id" => "11"}]}.to_json})

      expect(http).to receive(:request).with(
        :patch, "https://api.hubapi.com/crm/v3/objects/contacts/11",
        headers: hash_including("Accept" => "application/json"),
        body: satisfy do |body|
          json = JSON.parse(body)
          json["properties"] == {"firstname" => "S"}
        end
      ).and_return({status: 200, body: "{}"})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {"email" => "s@y.com", :firstname => "S"},
        id_property: :email
      )
      expect(id).to eq("11")
    end

    it "raises ApiError with generic message when body is non-JSON", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({status: 500, body: "<html>oops</html>"})

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {email: "x@y.com"},
          id_property: "email"
        )
      end.to raise_error(Etlify::ApiError, /HubSpot API request failed/)
    end

    it "wraps transport errors during update into TransportError", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({status: 200, body: {results: [{"id" => "1"}]}.to_json})

      expect(http).to receive(:request).with(
        :patch, /contacts\/1/, anything
      ).and_raise(StandardError.new("tcp reset"))

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {email: "x@y.com", firstname: "X"},
          id_property: "email"
        )
      end.to raise_error(Etlify::TransportError, /tcp reset/)
    end

    it "updates custom object when found", :aggregate_failures do
      custom_type = "p12345_myobject"

      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/#{custom_type}/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: kind_of(String)
      ).and_return({status: 200, body: {results: [{"id" => "42"}]}.to_json})

      expect(http).to receive(:request).with(
        :patch,
        "https://api.hubapi.com/crm/v3/objects/#{custom_type}/42",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy do |body|
          json = JSON.parse(body)
          json["properties"] == {"name" => "Custom A"}
        end
      ).and_return({status: 200, body: "{}"})

      id = adapter.upsert!(
        object_type: custom_type,
        payload: {unique_code: "ABC-001", name: "Custom A"},
        id_property: "unique_code"
      )
      expect(id).to eq("42")
    end

    context "when transport layer fails" do
      it "wraps the error into TransportError", :aggregate_failures do
        expect(http).to receive(:request).and_raise(StandardError.new("boom"))

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {email: "john@example.com", firstname: "John"},
            id_property: "email"
          )
        end.to raise_error(
          Etlify::TransportError, /HTTP transport error: StandardError: boom/
        )
      end
    end

    context "when transport layer raises an Etlify::Error" do
      it "wraps into TransportError and preserves inner class in message", :aggregate_failures do
        expect(http).to receive(:request).and_raise(
          Etlify::Error.new("boom", status: 500)
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {email: "john@example.com"},
            id_property: "email"
          )
        end.to raise_error(
          Etlify::TransportError, /HTTP transport error: Etlify::Error: boom/
        )
      end
    end

    context "when search returns 401" do
      it "raises Unauthorized", :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {
            status: 401,
            body: {
              message: "Invalid credentials",
              category: "INVALID_AUTHENTICATION",
              correlationId: "cid-1",
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {email: "john@example.com"},
            id_property: "email"
          )
        end.to raise_error(Etlify::Unauthorized, /Invalid credentials.*status=401/)
      end
    end

    context "when search returns 500" do
      it "raises ApiError", :aggregate_failures do
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {
            status: 500,
            body: {message: "Server error", category: "INTERNAL_ERROR"}.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {email: "john@example.com"},
            id_property: "email"
          )
        end.to raise_error(Etlify::ApiError, /Server error.*status=500/)
      end
    end

    context "when search returns 404" do
      it "treats as not found and proceeds to create", :aggregate_failures do
        # 1) Search -> 404 treated as "not found"
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 404, body: ""})

        # 2) Create succeeds
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] == {"email" => "j@e.com", "firstname" => "J"}
          end
        ).and_return({status: 201, body: {id: "777"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "j@e.com", firstname: "J"},
          id_property: "email"
        )
        expect(id).to eq("777")
      end
    end

    context "when update returns 429" do
      it "raises RateLimited", :aggregate_failures do
        # Search finds object
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {status: 200, body: {results: [{"id" => "1234"}]}.to_json}
        )

        # Update is rate limited
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {
            status: 429,
            body: {
              message: "Rate limit exceeded",
              category: "RATE_LIMITS",
              correlationId: "cid-2",
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {email: "john@example.com", firstname: "John"},
            id_property: "email"
          )
        end.to raise_error(
          Etlify::RateLimited, /Rate limit exceeded.*status=429.*correlationId=cid-2/
        )
      end
    end

    context "when create returns 409 (validation)" do
      it "raises ValidationFailed with details from payload", :aggregate_failures do
        # Search -> no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 200, body: {results: []}.to_json})

        # Create -> validation error
        error_payload = {
          message: "Property values were invalid",
          category: "VALIDATION_ERROR",
          correlationId: "cid-3",
          errors: [{message: "email must be unique", errorType: "CONFLICT"}],
        }

        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 409, body: error_payload.to_json})

        begin
          adapter.upsert!(
            object_type: "contacts",
            payload: {email: "dup@example.com", firstname: "Dup"},
            id_property: "email"
          )
          raise "expected to raise"
        rescue Etlify::ValidationFailed => error
          expect(error.message).to match(/Property values were invalid/)
          expect(error.status).to eq(409)
          expect(error.category).to eq("VALIDATION_ERROR")
          expect(error.correlation_id).to eq("cid-3")
          expect(error.details).to be_an(Array)
          expect(error.details.first["message"]).to eq("email must be unique")
        end
      end
    end

    context "when update returns 500" do
      it "raises ApiError", :aggregate_failures do
        # Search finds object
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return(
          {status: 200, body: {results: [{"id" => "1234"}]}.to_json}
        )

        # Update fails with 500
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 500, body: {message: "Internal error"}.to_json})

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {email: "john@example.com", firstname: "John"},
            id_property: "email"
          )
        end.to raise_error(Etlify::ApiError, /Internal error.*status=500/)
      end
    end

    it "sends standard JSON headers on create", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /\/crm\/v3\/objects\/deals/,
        headers: include(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json"
        ),
        body: kind_of(String)
      ).and_return({status: 201, body: {id: "d1"}.to_json})

      id = adapter.upsert!(
        object_type: "deals", payload: {dealname: "N", amount: 1_000}
      )
      expect(id).to eq("d1")
    end
  end

  describe "#delete!" do
    it "returns true on 2xx response", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 204, body: ""})

      expect(adapter.delete!(object_type: "contacts", crm_id: "1234")).to be true
    end

    it "returns false on non-2xx response", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 404, body: ""})

      expect(adapter.delete!(object_type: "contacts", crm_id: "1234")).to be false
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.delete!(object_type: "", crm_id: "1")
      end.to raise_error(ArgumentError)

      expect do
        adapter.delete!(object_type: "contacts", crm_id: nil)
      end.to raise_error(ArgumentError)
    end

    it "raises ApiError on 400 delete", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete, "https://api.hubapi.com/crm/v3/objects/contacts/1",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 400, body: {message: "Bad"}.to_json})

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1")
      end.to raise_error(Etlify::ApiError, /Bad/)
    end

    it "raises on blank crm_id" do
      expect do
        adapter.delete!(object_type: "contacts", crm_id: "")
      end.to raise_error(ArgumentError)
    end

    it "raises Unauthorized on 401", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 401, body: {message: "No auth"}.to_json})

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1234")
      end.to raise_error(Etlify::Unauthorized)
    end

    it "raises ApiError on 500", :aggregate_failures do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.hubapi.com/crm/v3/objects/contacts/1234",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: nil
      ).and_return({status: 500, body: {message: "Server down"}.to_json})

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1234")
      end.to raise_error(Etlify::ApiError, /Server down/)
    end

    it "wraps transport errors into TransportError", :aggregate_failures do
      expect(http).to receive(:request).and_raise(StandardError.new("network oops"))

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1234")
      end.to raise_error(Etlify::TransportError, /network oops/)
    end
  end
end
