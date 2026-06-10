require "rails_helper"
require "etlify/adapters/hubspot_v3_adapter"

RSpec.describe Etlify::Adapters::HubspotV3Adapter do
  let(:token) { "test-token" }
  let(:http)  { instance_double("HttpClient") }

  subject(:adapter) do
    described_class.new(access_token: token, http_client: http)
  end

  describe "#upsert!" do
    context "when object exists (search by match_property) for native type" do
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

        # 2) Update: the body is exactly the payload as provided
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
          payload: {firstname: "John"},
          match_property: "email",
          match_value: "john@example.com"
        )
        expect(id).to eq("1234")
      end
    end

    context "when crm_id is provided" do
      it "skips search and PATCHes directly with the payload as-is",
         :aggregate_failures do
        # Must NOT hit the /search endpoint
        expect(http).not_to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          anything
        )

        # Direct update on the provided crm_id: the payload is sent verbatim,
        # the matching property is neither injected nor stripped.
        expect(http).to receive(:request).with(
          :patch,
          "https://api.hubapi.com/crm/v3/objects/contacts/1234",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] ==
              {"email" => "kept@example.com", "firstname" => "John"}
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "kept@example.com", firstname: "John"},
          match_property: "email",
          match_value: nil,
          crm_id: "1234"
        )

        expect(id).to eq("1234")
      end
    end

    context "when object does not exist yet (native type)" do
      it "POSTs a new object with the match property injected", :aggregate_failures do
        # 1) Search → no results
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts/search",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: kind_of(String)
        ).and_return({status: 200, body: {results: []}.to_json})

        # 2) Create: payload + {match_property => match_value}
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
          payload: {firstname: "John"},
          match_property: "email",
          match_value: "john@example.com"
        )
        expect(id).to eq("5678")
      end
    end

    context "when match_property is blank" do
      it "raises ArgumentError", :aggregate_failures do
        expect do
          adapter.upsert!(
            object_type: "deals",
            payload: {dealname: "New deal"},
            match_property: nil,
            match_value: "New deal"
          )
        end.to raise_error(ArgumentError, /match_property/)

        expect do
          adapter.upsert!(
            object_type: "deals",
            payload: {dealname: "New deal"},
            match_property: "  ",
            match_value: "New deal"
          )
        end.to raise_error(ArgumentError, /match_property/)
      end
    end

    context "when match_value is blank and crm_id is unknown" do
      it "raises ArgumentError", :aggregate_failures do
        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {firstname: "John"},
            match_property: "email",
            match_value: nil
          )
        end.to raise_error(ArgumentError, /match_value/)

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {firstname: "John"},
            match_property: "email",
            match_value: "   "
          )
        end.to raise_error(ArgumentError, /match_value/)
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

        # 2) Create: the payload already carries the match property,
        # nothing is injected twice
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
          match_property: "unique_code",
          match_value: "ABC-001"
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

        # 2) Update: payload sent as-is
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
          payload: {firstname: "John"},
          match_property: "email",
          match_value: email_in
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

        # 2) Update: the matched contact keeps its primary email, the
        # payload does not carry one
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
          payload: {firstname: "A"},
          match_property: "email",
          match_value: "Alias+Promo@Example.com"
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

        # 2) Create: email should be injected lowercased in properties
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
          payload: {firstname: "J"},
          match_property: "email",
          match_value: "John+Tag@Example.com"
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
          payload: {firstname: "J"},
          match_property: "email",
          match_value: "John@Example.com"
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

      # Create includes both properties (stringified)
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
        match_property: "email",
        match_value: "a@b.com"
      )
      expect(id).to eq("314")
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.upsert!(
          object_type: "",
          payload: {},
          match_property: "email",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError)

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: "not a hash",
          match_property: "email",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError)
    end

    it "creates with the injected match property when the payload lacks it" do
      # 1) Search → no results
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts/search",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: kind_of(String)
      ).and_return({status: 200, body: {results: []}.to_json})

      # 2) Create: {match_property => match_value} injected alongside payload
      expect(http).to receive(:request).with(
        :post,
        "https://api.hubapi.com/crm/v3/objects/contacts",
        headers: hash_including(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json"
        ),
        body: satisfy do |b|
          JSON.parse(b)["properties"] == {"firstname" => "J", "email" => "j@e.com"}
        end
      ).and_return({status: 201, body: {id: "1001"}.to_json})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {firstname: "J"},
        match_property: "email",
        match_value: "j@e.com"
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
        payload: {firstname: "X"},
        match_property: "email",
        match_value: "x@y.com"
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
        payload: {firstname: "X"},
        match_property: "email",
        match_value: "x@y.com"
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
        payload: {firstname: "John"},
        match_property: "email",
        match_value: "john@example.com"
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
          payload: {firstname: "John"},
          match_property: "email",
          match_value: "john@example.com"
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
          payload: {firstname: "A"},
          match_property: "email",
          match_value: "a@b.com"
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
          payload: {firstname: "X"},
          match_property: "email",
          match_value: "x@y.com"
        )
      end.to raise_error(Etlify::Unauthorized, /Forbidden/)
    end

    it "does not write the matching property on update when absent from payload",
       :aggregate_failures do
      # This is the fix for the HubSpot primary-email overwrite bug: when a
      # contact is matched through a secondary email, the PATCH must not
      # rewrite the primary email with the platform value.
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
          props = JSON.parse(body)["properties"]
          props == {"firstname" => "S"} && !props.key?("email")
        end
      ).and_return({status: 200, body: "{}"})

      id = adapter.upsert!(
        object_type: "contacts",
        payload: {firstname: "S"},
        match_property: :email,
        match_value: "s@y.com"
      )
      expect(id).to eq("11")
    end

    context "at creation when the payload already carries the match property" do
      it "lets the payload value win over match_value (string key)",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :post, /contacts\/search/, anything
        ).and_return({status: 200, body: {results: []}.to_json})

        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] ==
              {"email" => "primary@example.com", "firstname" => "A"}
          end
        ).and_return({status: 201, body: {id: "21"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {"email" => "primary@example.com", "firstname" => "A"},
          match_property: "email",
          match_value: "secondary@example.com"
        )
        expect(id).to eq("21")
      end

      it "lets the payload value win over match_value (symbol key)",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :post, /contacts\/search/, anything
        ).and_return({status: 200, body: {results: []}.to_json})

        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] ==
              {"email" => "primary@example.com", "firstname" => "B"}
          end
        ).and_return({status: 201, body: {id: "22"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {email: "primary@example.com", firstname: "B"},
          match_property: "email",
          match_value: "secondary@example.com"
        )
        expect(id).to eq("22")
      end
    end

    it "raises ApiError with generic message when body is non-JSON", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /contacts\/search/, anything
      ).and_return({status: 500, body: "<html>oops</html>"})

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "X"},
          match_property: "email",
          match_value: "x@y.com"
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
          payload: {firstname: "X"},
          match_property: "email",
          match_value: "x@y.com"
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
        payload: {name: "Custom A"},
        match_property: "unique_code",
        match_value: "ABC-001"
      )
      expect(id).to eq("42")
    end

    context "when transport layer fails" do
      it "wraps the error into TransportError", :aggregate_failures do
        expect(http).to receive(:request).and_raise(StandardError.new("boom"))

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {firstname: "John"},
            match_property: "email",
            match_value: "john@example.com"
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
            payload: {firstname: "John"},
            match_property: "email",
            match_value: "john@example.com"
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
            payload: {firstname: "John"},
            match_property: "email",
            match_value: "john@example.com"
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
            payload: {firstname: "John"},
            match_property: "email",
            match_value: "john@example.com"
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

        # 2) Create succeeds with the injected match property
        expect(http).to receive(:request).with(
          :post,
          "https://api.hubapi.com/crm/v3/objects/contacts",
          headers: hash_including("Authorization" => "Bearer #{token}"),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["properties"] == {"firstname" => "J", "email" => "j@e.com"}
          end
        ).and_return({status: 201, body: {id: "777"}.to_json})

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {firstname: "J"},
          match_property: "email",
          match_value: "j@e.com"
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
            payload: {firstname: "John"},
            match_property: "email",
            match_value: "john@example.com"
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
            payload: {firstname: "Dup"},
            match_property: "email",
            match_value: "dup@example.com"
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
            payload: {firstname: "John"},
            match_property: "email",
            match_value: "john@example.com"
          )
        end.to raise_error(Etlify::ApiError, /Internal error.*status=500/)
      end
    end

    it "sends standard JSON headers on create", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post, /\/crm\/v3\/objects\/deals\/search/, anything
      ).and_return({status: 200, body: {results: []}.to_json})

      expect(http).to receive(:request).with(
        :post, "https://api.hubapi.com/crm/v3/objects/deals",
        headers: include(
          "Authorization" => "Bearer #{token}",
          "Content-Type" => "application/json",
          "Accept" => "application/json"
        ),
        body: kind_of(String)
      ).and_return({status: 201, body: {id: "d1"}.to_json})

      id = adapter.upsert!(
        object_type: "deals",
        payload: {amount: 1_000},
        match_property: "dealname",
        match_value: "N"
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

  describe "#batch_upsert!" do
    let(:upsert_url) { "https://api.hubapi.com/crm/v3/objects/contacts/batch/upsert" }

    it "POSTs to batch/upsert and returns IDs keyed by input value",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        upsert_url,
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |body|
          json = JSON.parse(body)
          inputs = json["inputs"]
          inputs.size == 2 &&
            inputs[0]["id"] == "john@example.com" &&
            inputs[0]["idProperty"] == "email" &&
            inputs[0]["properties"] == {"firstname" => "John"} &&
            inputs[1]["id"] == "jane@example.com" &&
            inputs[1]["idProperty"] == "email" &&
            inputs[1]["properties"] == {"firstname" => "Jane"}
        }
      ).and_return(
        {
          status: 200,
          body: {
            status: "COMPLETE",
            results: [
              {"id" => "101", "properties" => {"email" => "john@example.com"}},
              {"id" => "102", "properties" => {"email" => "jane@example.com"}},
            ],
          }.to_json,
        }
      )

      result = adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [
          {value: "john@example.com", properties: {firstname: "John"}},
          {value: "jane@example.com", properties: {firstname: "Jane"}},
        ],
        match_property: "email"
      )
      expect(result).to eq(
        "john@example.com" => "101",
        "jane@example.com" => "102"
      )
    end

    it "keys the mapping by input values as provided when HubSpot lowercases emails",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        upsert_url,
        headers: anything,
        body: satisfy { |body|
          input = JSON.parse(body)["inputs"].first
          input["id"] == "john@example.com" && input["idProperty"] == "email"
        }
      ).and_return(
        {
          status: 200,
          body: {
            status: "COMPLETE",
            results: [
              {"id" => "201", "properties" => {"email" => "john@example.com"}},
            ],
          }.to_json,
        }
      )

      result = adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [{value: "John@Example.com", properties: {firstname: "John"}}],
        match_property: "email"
      )
      expect(result).to eq("John@Example.com" => "201")
    end

    it "never injects the matching property into properties",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        upsert_url,
        headers: anything,
        body: satisfy { |body|
          props = JSON.parse(body)["inputs"].first["properties"]
          props == {"firstname" => "A"} && !props.key?("email")
        }
      ).and_return(
        {
          status: 200,
          body: {
            results: [{"id" => "1", "properties" => {"email" => "a@b.com"}}],
          }.to_json,
        }
      )

      adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [{value: "a@b.com", properties: {firstname: "A"}}],
        match_property: "email"
      )
    end

    it "slices inputs into batches of BATCH_MAX_SIZE",
       :aggregate_failures do
      inputs = (1..150).map do |i|
        {value: "user#{i}@example.com", properties: {firstname: "User#{i}"}}
      end

      call_count = 0
      expect(http).to receive(:request).with(
        :post, upsert_url, anything
      ).twice do
        call_count += 1
        email = "user#{(call_count - 1) * 100 + 1}@example.com"
        {
          status: 200,
          body: {
            status: "COMPLETE",
            results: [{"id" => call_count.to_s, "properties" => {"email" => email}}],
          }.to_json,
        }
      end

      result = adapter.batch_upsert!(
        object_type: "contacts",
        inputs: inputs,
        match_property: "email"
      )
      expect(result).to eq(
        "user1@example.com" => "1",
        "user101@example.com" => "2"
      )
    end

    it "stringifies symbol keys in properties",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        upsert_url,
        headers: anything,
        body: satisfy { |body|
          json = JSON.parse(body)
          props = json["inputs"].first["properties"]
          props.keys.all? { |k| k.is_a?(String) }
        }
      ).and_return(
        {
          status: 200,
          body: {
            results: [{"id" => "1"}],
          }.to_json,
        }
      )

      adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [{value: "a@b.com", properties: {firstname: "A"}}],
        match_property: "email"
      )
    end

    it "accepts string keys in inputs",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        upsert_url,
        headers: anything,
        body: satisfy { |body|
          input = JSON.parse(body)["inputs"].first
          input["id"] == "a@b.com" &&
            input["properties"] == {"firstname" => "A"}
        }
      ).and_return(
        {
          status: 200,
          body: {
            results: [{"id" => "1", "properties" => {"email" => "a@b.com"}}],
          }.to_json,
        }
      )

      result = adapter.batch_upsert!(
        object_type: "contacts",
        inputs: [{"value" => "a@b.com", "properties" => {"firstname" => "A"}}],
        match_property: "email"
      )
      expect(result).to eq("a@b.com" => "1")
    end

    it "works with custom object types" do
      custom_url = "https://api.hubapi.com/crm/v3/objects/p12345_myobject/batch/upsert"

      expect(http).to receive(:request).with(
        :post, custom_url, anything
      ).and_return(
        {
          status: 200,
          body: {
            results: [{"id" => "42", "properties" => {"ref" => "ABC"}}],
          }.to_json,
        }
      )

      result = adapter.batch_upsert!(
        object_type: "p12345_myobject",
        inputs: [{value: "ABC", properties: {name: "Test"}}],
        match_property: "ref"
      )
      expect(result).to eq("ABC" => "42")
    end

    it "raises ArgumentError on invalid arguments",
       :aggregate_failures do
      expect do
        adapter.batch_upsert!(
          object_type: "",
          inputs: [{value: "a@b.com", properties: {}}],
          match_property: "email"
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "a@b.com", properties: {}}],
          match_property: nil
        )
      end.to raise_error(ArgumentError, /match_property/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: "not array",
          match_property: "email"
        )
      end.to raise_error(ArgumentError, /inputs/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [],
          match_property: "email"
        )
      end.to raise_error(ArgumentError, /inputs/)
    end

    it "raises ArgumentError when an input has a blank value",
       :aggregate_failures do
      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "  ", properties: {firstname: "A"}}],
          match_property: "email"
        )
      end.to raise_error(ArgumentError, /value/)

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{properties: {firstname: "A"}}],
          match_property: "email"
        )
      end.to raise_error(ArgumentError, /value/)
    end

    it "raises Unauthorized on 401" do
      expect(http).to receive(:request).and_return(
        {
          status: 401,
          body: {
            message: "Invalid token",
            category: "INVALID_AUTHENTICATION",
          }.to_json,
        }
      )

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "a@b.com", properties: {firstname: "A"}}],
          match_property: "email"
        )
      end.to raise_error(Etlify::Unauthorized, /Invalid token/)
    end

    it "raises RateLimited on 429" do
      expect(http).to receive(:request).and_return(
        {
          status: 429,
          body: {
            message: "Too many requests",
            category: "RATE_LIMITS",
          }.to_json,
        }
      )

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "a@b.com", properties: {firstname: "A"}}],
          match_property: "email"
        )
      end.to raise_error(Etlify::RateLimited, /Too many requests/)
    end

    it "raises ApiError on 500" do
      expect(http).to receive(:request).and_return(
        {status: 500, body: {message: "Server down"}.to_json}
      )

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "a@b.com", properties: {firstname: "A"}}],
          match_property: "email"
        )
      end.to raise_error(Etlify::ApiError, /Server down/)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(
        StandardError.new("connection reset")
      )

      expect do
        adapter.batch_upsert!(
          object_type: "contacts",
          inputs: [{value: "a@b.com", properties: {firstname: "A"}}],
          match_property: "email"
        )
      end.to raise_error(Etlify::TransportError, /connection reset/)
    end
  end

  describe "#batch_update!" do
    let(:update_url) { "https://api.hubapi.com/crm/v3/objects/contacts/batch/update" }

    it "POSTs to batch/update targeting each object by crm_id",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        update_url,
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |body|
          json = JSON.parse(body)
          inputs = json["inputs"]
          inputs.size == 2 &&
            inputs[0]["id"] == "101" &&
            inputs[0]["properties"] == {"email" => "john@example.com"} &&
            !inputs[0].key?("idProperty") &&
            inputs[1]["id"] == "102" &&
            inputs[1]["properties"] == {"email" => "jane@example.com"}
        }
      ).and_return({status: 200, body: {results: []}.to_json})

      result = adapter.batch_update!(
        object_type: "contacts",
        records: [
          {crm_id: "101", properties: {email: "john@example.com"}},
          {crm_id: "102", properties: {email: "jane@example.com"}},
        ]
      )

      # Identity mapping built from inputs (crm_id does not change on update).
      expect(result).to eq("101" => "101", "102" => "102")
    end

    it "slices records into batches of BATCH_MAX_SIZE", :aggregate_failures do
      records = (1..150).map { |i| {crm_id: i.to_s, properties: {n: i}} }

      expect(http).to receive(:request).with(
        :post, update_url, anything
      ).twice.and_return({status: 200, body: {results: []}.to_json})

      result = adapter.batch_update!(object_type: "contacts", records: records)
      expect(result.size).to eq(150)
    end

    it "stringifies symbol keys in properties", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        update_url,
        headers: anything,
        body: satisfy { |body|
          props = JSON.parse(body)["inputs"].first["properties"]
          props.keys.all? { |k| k.is_a?(String) }
        }
      ).and_return({status: 200, body: {results: []}.to_json})

      adapter.batch_update!(
        object_type: "contacts",
        records: [{crm_id: "1", properties: {email: "a@b.com", firstname: "A"}}]
      )
    end

    it "raises NotFound when HubSpot returns 404 for a dead crm_id" do
      allow(http).to receive(:request).and_return(
        {status: 404, body: {message: "not found", category: "OBJECT_NOT_FOUND"}.to_json}
      )

      expect do
        adapter.batch_update!(
          object_type: "contacts",
          records: [{crm_id: "dead", properties: {email: "a@b.com"}}]
        )
      end.to raise_error(Etlify::NotFound)
    end

    it "raises ArgumentError on an empty records array" do
      expect do
        adapter.batch_update!(object_type: "contacts", records: [])
      end.to raise_error(ArgumentError, /non-empty Array/)
    end
  end

  describe "#batch_delete!" do
    let(:archive_url) { "https://api.hubapi.com/crm/v3/objects/contacts/batch/archive" }

    it "POSTs to batch/archive and returns true",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        archive_url,
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy { |body|
          json = JSON.parse(body)
          json["inputs"] == [
            {"id" => "101"},
            {"id" => "102"},
            {"id" => "103"},
          ]
        }
      ).and_return({status: 204, body: ""})

      result = adapter.batch_delete!(
        object_type: "contacts",
        crm_ids: ["101", "102", "103"]
      )
      expect(result).to be true
    end

    it "slices crm_ids into batches of BATCH_MAX_SIZE",
       :aggregate_failures do
      ids = (1..150).map(&:to_s)

      expect(http).to receive(:request).with(
        :post, archive_url, anything
      ).twice.and_return({status: 204, body: ""})

      result = adapter.batch_delete!(
        object_type: "contacts",
        crm_ids: ids
      )
      expect(result).to be true
    end

    it "raises ArgumentError on invalid arguments",
       :aggregate_failures do
      expect do
        adapter.batch_delete!(
          object_type: "",
          crm_ids: ["1"]
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_delete!(
          object_type: "contacts",
          crm_ids: "not array"
        )
      end.to raise_error(ArgumentError, /crm_ids/)

      expect do
        adapter.batch_delete!(
          object_type: "contacts",
          crm_ids: []
        )
      end.to raise_error(ArgumentError, /crm_ids/)
    end

    it "raises Unauthorized on 401" do
      expect(http).to receive(:request).and_return(
        {
          status: 401,
          body: {
            message: "Invalid token",
            category: "INVALID_AUTHENTICATION",
          }.to_json,
        }
      )

      expect do
        adapter.batch_delete!(
          object_type: "contacts",
          crm_ids: ["1"]
        )
      end.to raise_error(Etlify::Unauthorized, /Invalid token/)
    end

    it "raises ApiError on 500" do
      expect(http).to receive(:request).and_return(
        {status: 500, body: {message: "Server down"}.to_json}
      )

      expect do
        adapter.batch_delete!(
          object_type: "contacts",
          crm_ids: ["1"]
        )
      end.to raise_error(Etlify::ApiError, /Server down/)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(
        StandardError.new("dns failure")
      )

      expect do
        adapter.batch_delete!(
          object_type: "contacts",
          crm_ids: ["1"]
        )
      end.to raise_error(Etlify::TransportError, /dns failure/)
    end
  end
end
