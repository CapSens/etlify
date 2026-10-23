require "rails_helper"
require "etlify/adapters/brevo_adapter"

RSpec.describe Etlify::Adapters::BrevoAdapter do
  let(:api_key) { "xkeysib-test" }
  let(:http)    { instance_double("HttpClient") }

  subject(:adapter) do
    described_class.new(api_key: api_key, http_client: http)
  end

  def json_response(status, body = nil)
    {status: status, body: body.nil? ? "" : body.to_json}
  end

  def json_body(expected)
    satisfy { |body| JSON.parse(body) == expected }
  end

  describe "#initialize" do
    it "sends the api-key and JSON headers to the v3 base URL" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.brevo.com/v3/contacts/42?identifierType=contact_id",
        headers: {
          "api-key" => "xkeysib-test",
          "Content-Type" => "application/json",
          "Accept" => "application/json",
        },
        body: nil
      ).and_return(json_response(204))

      adapter.delete!(object_type: "contacts", crm_id: "42")
    end

    it "raises when api_key is blank" do
      expect do
        described_class.new(api_key: " ")
      end.to raise_error(ArgumentError, "api_key must be a non-empty String")
    end
  end

  describe "#upsert!" do
    let(:get_url) do
      "https://api.brevo.com/v3/contacts/user-1?identifierType=ext_id"
    end

    context "when crm_id is known" do
      it "PUTs the payload as-is by contact_id, without search",
         :aggregate_failures do
        expect(http).not_to receive(:request).with(:get, anything, anything)
        expect(http).to receive(:request).with(
          :put,
          "https://api.brevo.com/v3/contacts/42?identifierType=contact_id",
          headers: hash_including("api-key" => "xkeysib-test"),
          body: json_body({"attributes" => {"PRENOM" => "Jane"}})
        ).and_return(json_response(204))

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {attributes: {PRENOM: "Jane"}},
          match_property: "ext_id",
          match_value: "user-1",
          crm_id: "42"
        )

        expect(id).to eq("42")
      end

      context "when the contact no longer exists" do
        let(:not_found) do
          json_response(
            404, {code: "document_not_found", message: "Contact does not exist"}
          )
        end

        before do
          allow(http).to receive(:request).with(
            :put,
            "https://api.brevo.com/v3/contacts/999?identifierType=contact_id",
            anything
          ).and_return(not_found)
        end

        it "PUTs the payload on the contact found by match_property",
           :aggregate_failures do
          expect(http).to receive(:request).with(
            :get, get_url, headers: anything, body: nil
          ).and_return(json_response(200, {id: 2}))
          expect(http).to receive(:request).with(
            :put,
            "https://api.brevo.com/v3/contacts/2?identifierType=contact_id",
            headers: anything,
            body: json_body({"attributes" => {"PRENOM" => "Jane"}})
          ).and_return(json_response(204))

          id = adapter.upsert!(
            object_type: "contacts",
            payload: {attributes: {PRENOM: "Jane"}},
            match_property: "ext_id",
            match_value: "user-1",
            crm_id: "999"
          )

          expect(id).to eq("2")
        end

        it "POSTs the payload when no contact matches either",
           :aggregate_failures do
          expect(http).to receive(:request).with(
            :get, get_url, headers: anything, body: nil
          ).and_return(json_response(404))
          expect(http).to receive(:request).with(
            :post,
            "https://api.brevo.com/v3/contacts",
            headers: anything,
            body: json_body(
              {"attributes" => {"PRENOM" => "Jane"}, "ext_id" => "user-1"}
            )
          ).and_return(json_response(201, {id: 3}))

          id = adapter.upsert!(
            object_type: "contacts",
            payload: {attributes: {PRENOM: "Jane"}},
            match_property: "ext_id",
            match_value: "user-1",
            crm_id: "999"
          )

          expect(id).to eq("3")
        end

        it "raises Etlify::NotFound when match_value is blank" do
          expect do
            adapter.upsert!(
              object_type: "contacts",
              payload: {attributes: {PRENOM: "Jane"}},
              match_property: "ext_id",
              match_value: " ",
              crm_id: "999"
            )
          end.to raise_error(
            Etlify::NotFound,
            "Contact does not exist (status=404, path=/contacts/999, " \
            "code=document_not_found)"
          )
        end
      end
    end

    context "when the contact is found by match_property" do
      it "PUTs the payload as-is on the found id and never writes the " \
         "matching property", :aggregate_failures do
        expect(http).to receive(:request).with(
          :get,
          get_url,
          headers: hash_including("api-key" => "xkeysib-test"),
          body: nil
        ).and_return(json_response(200, {id: 2, email: "jane@example.com"}))
        expect(http).to receive(:request).with(
          :put,
          "https://api.brevo.com/v3/contacts/2?identifierType=contact_id",
          headers: anything,
          body: json_body({"attributes" => {"PRENOM" => "Jane"}})
        ).and_return(json_response(204))

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {attributes: {PRENOM: "Jane"}},
          match_property: "ext_id",
          match_value: "user-1"
        )

        expect(id).to eq("2")
      end

      it "searches an email matching value lowercased and URL-encoded" do
        expect(http).to receive(:request).with(
          :get,
          "https://api.brevo.com/v3/contacts/jane%2Btest%40example.com" \
          "?identifierType=email_id",
          headers: anything,
          body: nil
        ).and_return(json_response(200, {id: 2}))
        allow(http).to receive(:request).with(:put, anything, anything)
          .and_return(json_response(204))

        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "email",
          match_value: " Jane+Test@Example.com "
        )
      end
    end

    context "when the contact is not found" do
      it "POSTs the payload with the matching property injected and " \
         "returns the new id", :aggregate_failures do
        allow(http).to receive(:request).with(:get, get_url, anything)
          .and_return(json_response(404, {code: "document_not_found"}))
        expect(http).to receive(:request).with(
          :post,
          "https://api.brevo.com/v3/contacts",
          headers: anything,
          body: json_body(
            {
              "attributes" => {"EMAIL" => "jane@example.com", "PRENOM" => "Jane"},
              "ext_id" => "user-1",
            }
          )
        ).and_return(json_response(201, {id: 3}))

        id = adapter.upsert!(
          object_type: "contacts",
          payload: {attributes: {EMAIL: "jane@example.com", PRENOM: "Jane"}},
          match_property: "ext_id",
          match_value: "user-1"
        )

        expect(id).to eq("3")
      end

      it "keeps the payload value when it already carries the matching " \
         "property, lowercasing a payload email" do
        allow(http).to receive(:request).with(:get, anything, anything)
          .and_return(json_response(404))
        expect(http).to receive(:request).with(
          :post,
          "https://api.brevo.com/v3/contacts",
          headers: anything,
          body: json_body({"email" => "payload@example.com"})
        ).and_return(json_response(201, {id: 3}))

        adapter.upsert!(
          object_type: "contacts",
          payload: {email: "Payload@Example.com"},
          match_property: "email",
          match_value: "match@example.com"
        )
      end

      it "raises Etlify::ApiError when another contact holds the email" do
        allow(http).to receive(:request).with(:get, anything, anything)
          .and_return(json_response(404))
        allow(http).to receive(:request).with(:post, anything, anything)
          .and_return(
            json_response(
              400,
              {
                code: "duplicate_parameter",
                message: "Unable to create contact, email is already " \
                         "associated with another Contact",
              }
            )
          )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {attributes: {EMAIL: "jane@example.com"}},
            match_property: "ext_id",
            match_value: "user-1"
          )
        end.to raise_error(
          Etlify::ApiError,
          "Unable to create contact, email is already associated with " \
          "another Contact (status=400, path=/contacts, code=duplicate_parameter)"
        )
      end

      it "returns nil when Brevo answers 2xx without an id" do
        allow(http).to receive(:request).with(:get, anything, anything)
          .and_return(json_response(404))
        allow(http).to receive(:request).with(:post, anything, anything)
          .and_return(json_response(204))

        expect(
          adapter.upsert!(
            object_type: "contacts",
            payload: {},
            match_property: "ext_id",
            match_value: "user-1"
          )
        ).to be_nil
      end
    end

    it "raises when the search fails with another error" do
      allow(http).to receive(:request).and_return(
        json_response(401, {code: "unauthorized", message: "Key not found"})
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "ext_id",
          match_value: "user-1"
        )
      end.to raise_error(
        Etlify::Unauthorized,
        "Key not found (status=401, path=/contacts/user-1, code=unauthorized)"
      )
    end

    it "raises when match_value is blank and crm_id is unknown" do
      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "ext_id",
          match_value: " "
        )
      end.to raise_error(
        ArgumentError, "match_value must be provided when crm_id is unknown"
      )
    end

    it "raises on an unsupported object_type" do
      expect do
        adapter.upsert!(
          object_type: "companies",
          payload: {},
          match_property: "ext_id",
          match_value: "c-1"
        )
      end.to raise_error(ArgumentError, 'object_type must be one of ["contacts"]')
    end

    it "raises on an unsupported match_property" do
      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "id",
          match_value: "1"
        )
      end.to raise_error(
        ArgumentError, 'match_property must be one of ["email", "ext_id"]'
      )
    end

    it "raises when payload is not a Hash" do
      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: [],
          match_property: "ext_id",
          match_value: "user-1"
        )
      end.to raise_error(ArgumentError, "payload must be a Hash")
    end
  end

  describe "#batch_delete!" do
    it "deletes each id and returns true" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.brevo.com/v3/contacts/1?identifierType=contact_id",
        headers: anything,
        body: nil
      ).and_return(json_response(204))
      expect(http).to receive(:request).with(
        :delete,
        "https://api.brevo.com/v3/contacts/2?identifierType=contact_id",
        headers: anything,
        body: nil
      ).and_return(json_response(404))

      expect(
        adapter.batch_delete!(object_type: "contacts", crm_ids: ["1", 2])
      ).to be(true)
    end

    it "raises on the first failure" do
      allow(http).to receive(:request).and_return(
        json_response(500, {message: "Internal error"})
      )

      expect do
        adapter.batch_delete!(object_type: "contacts", crm_ids: ["1", "2"])
      end.to raise_error(
        Etlify::ApiError, "Internal error (status=500, path=/contacts/1)"
      )
    end

    it "raises when crm_ids is empty" do
      expect do
        adapter.batch_delete!(object_type: "contacts", crm_ids: [])
      end.to raise_error(ArgumentError, "crm_ids must be a non-empty Array")
    end
  end

  describe "#delete!" do
    it "returns true on 2xx" do
      allow(http).to receive(:request).and_return(json_response(204))

      expect(adapter.delete!(object_type: "contacts", crm_id: "42")).to be(true)
    end

    it "returns false on 404" do
      allow(http).to receive(:request).and_return(
        json_response(404, {code: "document_not_found"})
      )

      expect(adapter.delete!(object_type: "contacts", crm_id: "42")).to be(false)
    end

    it "raises on other errors" do
      allow(http).to receive(:request).and_return(
        json_response(500, {message: "Internal error"})
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "42")
      end.to raise_error(
        Etlify::ApiError, "Internal error (status=500, path=/contacts/42)"
      )
    end

    it "raises when crm_id is blank" do
      expect do
        adapter.delete!(object_type: "contacts", crm_id: nil)
      end.to raise_error(ArgumentError, "crm_id must be provided")
    end
  end

  describe "#track_event!" do
    it "POSTs the event and returns true", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "https://api.brevo.com/v3/events",
        headers: hash_including("api-key" => "xkeysib-test"),
        body: json_body(
          {
            "event_name" => "Inscription",
            "identifiers" => {"contact_id" => 42},
            "event_properties" => {"prenom" => "Jane", "lie_cgp" => true},
            "contact_properties" => {"PRENOM" => "Jane"},
            "event_date" => "2026-10-16T09:00:00Z",
          }
        )
      ).and_return(json_response(204))

      result = adapter.track_event!(
        name: "Inscription",
        identifier_property: "crm_id",
        identifier_value: "42",
        properties: {prenom: "Jane", lie_cgp: true},
        contact_properties: {PRENOM: "Jane"},
        occurred_at: Time.utc(2026, 10, 16, 9, 0, 0)
      )

      expect(result).to be(true)
    end

    it "identifies the contact by email or ext_id" do
      expect(http).to receive(:request).with(
        :post,
        "https://api.brevo.com/v3/events",
        headers: anything,
        body: json_body(
          {
            "event_name" => "Inscription",
            "identifiers" => {"email_id" => "jane@example.com"},
          }
        )
      ).and_return(json_response(204))
      expect(http).to receive(:request).with(
        :post,
        "https://api.brevo.com/v3/events",
        headers: anything,
        body: json_body(
          {"event_name" => "Inscription", "identifiers" => {"ext_id" => "user-1"}}
        )
      ).and_return(json_response(204))

      adapter.track_event!(
        name: "Inscription",
        identifier_property: "email",
        identifier_value: "Jane@Example.com"
      )
      adapter.track_event!(
        name: "Inscription",
        identifier_property: "ext_id",
        identifier_value: "user-1"
      )
    end

    it "passes a string occurred_at through" do
      expect(http).to receive(:request).with(
        :post,
        "https://api.brevo.com/v3/events",
        headers: anything,
        body: json_body(
          {
            "event_name" => "Inscription",
            "identifiers" => {"ext_id" => "user-1"},
            "event_date" => "2026-10-16T11:00:00+02:00",
          }
        )
      ).and_return(json_response(204))

      adapter.track_event!(
        name: "Inscription",
        identifier_property: "ext_id",
        identifier_value: "user-1",
        occurred_at: "2026-10-16T11:00:00+02:00"
      )
    end

    it "raises on an invalid event name" do
      expect do
        adapter.track_event!(
          name: "Bienvenue !",
          identifier_property: "ext_id",
          identifier_value: "user-1"
        )
      end.to raise_error(
        ArgumentError,
        "name must be 1 to 255 alphanumeric, '-' or '_' characters"
      )
    end

    it "raises on an unsupported identifier_property" do
      expect do
        adapter.track_event!(
          name: "Inscription",
          identifier_property: "phone",
          identifier_value: "0600000000"
        )
      end.to raise_error(
        ArgumentError,
        'identifier_property must be one of ["email", "ext_id", "crm_id"]'
      )
    end

    it "raises when identifier_value is blank" do
      expect do
        adapter.track_event!(
          name: "Inscription",
          identifier_property: "ext_id",
          identifier_value: nil
        )
      end.to raise_error(ArgumentError, "identifier_value must be provided")
    end

    it "raises Etlify::ApiError on a 400" do
      allow(http).to receive(:request).and_return(
        json_response(400, {code: "invalid_parameter", message: "Invalid event"})
      )

      expect do
        adapter.track_event!(
          name: "Inscription",
          identifier_property: "ext_id",
          identifier_value: "user-1"
        )
      end.to raise_error(
        Etlify::ApiError,
        "Invalid event (status=400, path=/events, code=invalid_parameter)"
      )
    end
  end

  describe "error mapping" do
    {
      401 => Etlify::Unauthorized,
      403 => Etlify::Unauthorized,
      404 => Etlify::NotFound,
      409 => Etlify::ValidationFailed,
      422 => Etlify::ValidationFailed,
      429 => Etlify::RateLimited,
      500 => Etlify::ApiError,
    }.each do |status, error_class|
      it "maps #{status} to #{error_class}" do
        allow(http).to receive(:request).and_return(
          json_response(status, {message: "failure"})
        )

        expect do
          adapter.upsert!(
            object_type: "contacts",
            payload: {},
            match_property: "ext_id",
            match_value: "user-1"
          )
        end.to raise_error(error_class)
      end
    end

    it "wraps transport failures into Etlify::TransportError" do
      allow(http).to receive(:request).and_raise(Net::ReadTimeout)

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "ext_id",
          match_value: "user-1"
        )
      end.to raise_error(
        Etlify::TransportError,
        "HTTP transport error: Net::ReadTimeout: Net::ReadTimeout"
      )
    end

    it "keeps the error code and body on the raised error",
       :aggregate_failures do
      allow(http).to receive(:request).and_return(
        json_response(
          400, {code: "duplicate_parameter", message: "Duplicate"}
        )
      )

      expect do
        adapter.upsert!(
          object_type: "contacts",
          payload: {},
          match_property: "ext_id",
          match_value: "user-1"
        )
      end.to raise_error(Etlify::ApiError) { |error|
        expect(error.status).to eq(400)
        expect(error.code).to eq("duplicate_parameter")
        expect(error.details).to eq(
          {"code" => "duplicate_parameter", "message" => "Duplicate"}
        )
      }
    end
  end

  describe "rate limiting" do
    it "throttles every HTTP call through the injected limiter",
       :aggregate_failures do
      limiter = instance_double(Etlify::RateLimiter)
      adapter.rate_limiter = limiter
      allow(http).to receive(:request).and_return(json_response(204))

      expect(limiter).to receive(:throttle!).twice

      adapter.delete!(object_type: "contacts", crm_id: "42")
      adapter.track_event!(
        name: "Inscription",
        identifier_property: "ext_id",
        identifier_value: "user-1"
      )

      expect(adapter.rate_limiter).to eq(limiter)
    end
  end

  describe "with Etlify::Synchronizer" do
    let(:user) { User.create!(email: "jane@example.com", full_name: "Jane") }
    let(:adapter_conf) do
      {
        brevo: {
          adapter: adapter,
          match_by: {property: :ext_id, value: ->(u) { "user-#{u.id}" }},
          crm_object_type: "contacts",
        },
      }
    end

    before do
      Etlify.configure do |c|
        c.digest_strategy = Etlify::Digest.method(:stable_sha256)
      end
      allow(User).to receive(:etlify_crms).and_return(adapter_conf)
    end

    it "stores the Brevo id, then skips the call when nothing changed",
       :aggregate_failures do
      expect(http).to receive(:request).with(:get, anything, anything)
        .once.and_return(json_response(404))
      expect(http).to receive(:request).with(:post, anything, anything)
        .once.and_return(json_response(201, {id: 7}))

      expect(Etlify::Synchronizer.call(user, crm_name: :brevo)).to eq(:synced)
      line = user.crm_synchronisations.find_by(crm_name: "brevo")
      expect(line.crm_id).to eq("7")

      expect(Etlify::Synchronizer.call(user, crm_name: :brevo))
        .to eq(:not_modified)
    end

    it "replaces a dead crm_id with the id of the recreated contact",
       :aggregate_failures do
      user.crm_synchronisations.create!(crm_name: "brevo", crm_id: "498")
      allow(http).to receive(:request).with(:put, anything, anything)
        .and_return(json_response(404, {code: "document_not_found"}))
      allow(http).to receive(:request).with(:get, anything, anything)
        .and_return(json_response(404))
      allow(http).to receive(:request).with(:post, anything, anything)
        .and_return(json_response(201, {id: 8}))

      expect(Etlify::Synchronizer.call(user, crm_name: :brevo)).to eq(:synced)
      line = user.crm_synchronisations.find_by(crm_name: "brevo")
      expect(line.crm_id).to eq("8")
      expect(line.last_error).to be_nil
    end
  end
end
