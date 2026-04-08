require "rails_helper"

RSpec.describe Etlify::Adapters::BrevoAdapter do
  let(:api_key) { "xkeysib-test-key" }
  let(:http) { instance_double("HttpClient") }
  let(:adapter) { described_class.new(api_key: api_key, http_client: http) }

  def json_response(status, body = nil)
    {status: status, body: body ? body.to_json : ""}
  end

  # ------------------------------------------------------------------ #
  # Contacts
  # ------------------------------------------------------------------ #

  describe "#upsert! for contacts" do
    it "creates a contact when not found", :aggregate_failures do
      # Search returns 404
      expect(http).to receive(:request).with(
        :get, anything, headers: anything, body: nil
      ).and_return(json_response(404))

      # Create
      expect(http).to receive(:request).with(
        :post,
        "https://api.brevo.com/v3/contacts",
        headers: hash_including("api-key" => api_key),
        body: satisfy { |b|
          json = JSON.parse(b)
          json["email"] == "john@example.com"
        }
      ).and_return(json_response(201, {id: 42}))

      result = adapter.upsert!(
        object_type: "contacts",
        payload: {email: "john@example.com", FIRSTNAME: "John"},
        id_property: "email"
      )
      expect(result).to eq("42")
    end

    it "updates an existing contact", :aggregate_failures do
      # Search finds contact
      expect(http).to receive(:request).with(
        :get, anything, headers: anything, body: nil
      ).and_return(json_response(200, {id: 99, email: "jane@example.com"}))

      # Update
      expect(http).to receive(:request).with(
        :put,
        "https://api.brevo.com/v3/contacts/99",
        headers: hash_including("api-key" => api_key),
        body: anything
      ).and_return(json_response(204))

      result = adapter.upsert!(
        object_type: "contacts",
        payload: {email: "jane@example.com", LASTNAME: "Doe"},
        id_property: "email"
      )
      expect(result).to eq("99")
    end

    it "updates directly when crm_id is provided" do
      expect(http).to receive(:request).with(
        :put,
        "https://api.brevo.com/v3/contacts/55",
        headers: anything,
        body: anything
      ).and_return(json_response(204))

      result = adapter.upsert!(
        object_type: "contacts",
        payload: {FIRSTNAME: "Updated"},
        crm_id: "55"
      )
      expect(result).to eq("55")
    end

    it "sends identifierType=email_id for email lookups" do
      expect(http).to receive(:request).with(
        :get,
        satisfy { |url| url.include?("identifierType=email_id") },
        headers: anything,
        body: nil
      ).and_return(json_response(404))

      expect(http).to receive(:request).with(
        :post, anything, headers: anything, body: anything
      ).and_return(json_response(201, {id: 1}))

      adapter.upsert!(
        object_type: "contacts",
        payload: {email: "test@example.com"},
        id_property: "email"
      )
    end

    it "sends identifierType=ext_id for ext_id lookups" do
      expect(http).to receive(:request).with(
        :get,
        satisfy { |url| url.include?("identifierType=ext_id") },
        headers: anything,
        body: nil
      ).and_return(json_response(200, {id: 77}))

      expect(http).to receive(:request).with(
        :put, anything, headers: anything, body: anything
      ).and_return(json_response(204))

      adapter.upsert!(
        object_type: "contacts",
        payload: {ext_id: "user-123", FIRSTNAME: "Alice"},
        id_property: "ext_id"
      )
    end

    it "stringifies symbol keys in payload" do
      expect(http).to receive(:request).with(
        :get, anything, headers: anything, body: nil
      ).and_return(json_response(404))

      expect(http).to receive(:request).with(
        :post,
        anything,
        headers: anything,
        body: satisfy { |b|
          json = JSON.parse(b)
          json["email"] == "sym@example.com" &&
            json["attributes"].is_a?(Hash) &&
            json["attributes"].key?("FIRSTNAME")
        }
      ).and_return(json_response(201, {id: 10}))

      adapter.upsert!(
        object_type: "contacts",
        payload: {email: "sym@example.com", FIRSTNAME: "Sym"},
        id_property: "email"
      )
    end
  end

  # ------------------------------------------------------------------ #
  # Companies
  # ------------------------------------------------------------------ #

  describe "#upsert! for companies" do
    it "creates a company when no crm_id" do
      expect(http).to receive(:request).with(
        :post,
        "https://api.brevo.com/v3/companies",
        headers: hash_including("api-key" => api_key),
        body: satisfy { |b|
          json = JSON.parse(b)
          json["name"] == "CapSens"
        }
      ).and_return(json_response(201, {id: "comp-1"}))

      result = adapter.upsert!(
        object_type: "companies",
        payload: {name: "CapSens", industry: "fintech"}
      )
      expect(result).to eq("comp-1")
    end

    it "updates a company when crm_id is provided" do
      expect(http).to receive(:request).with(
        :patch,
        "https://api.brevo.com/v3/companies/comp-1",
        headers: anything,
        body: anything
      ).and_return(json_response(204))

      result = adapter.upsert!(
        object_type: "companies",
        payload: {name: "CapSens Updated"},
        crm_id: "comp-1"
      )
      expect(result).to eq("comp-1")
    end
  end

  # ------------------------------------------------------------------ #
  # Deals
  # ------------------------------------------------------------------ #

  describe "#upsert! for deals" do
    it "creates a deal when no crm_id" do
      expect(http).to receive(:request).with(
        :post,
        "https://api.brevo.com/v3/crm/deals",
        headers: anything,
        body: satisfy { |b|
          json = JSON.parse(b)
          json["name"] == "Big Deal"
        }
      ).and_return(json_response(201, {id: "deal-1"}))

      result = adapter.upsert!(
        object_type: "deals",
        payload: {deal_name: "Big Deal", amount: 10_000}
      )
      expect(result).to eq("deal-1")
    end

    it "updates a deal when crm_id is provided" do
      expect(http).to receive(:request).with(
        :patch,
        "https://api.brevo.com/v3/crm/deals/deal-1",
        headers: anything,
        body: anything
      ).and_return(json_response(204))

      result = adapter.upsert!(
        object_type: "deals",
        payload: {deal_name: "Updated Deal"},
        crm_id: "deal-1"
      )
      expect(result).to eq("deal-1")
    end
  end

  # ------------------------------------------------------------------ #
  # Delete
  # ------------------------------------------------------------------ #

  describe "#delete!" do
    it "returns true on success" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.brevo.com/v3/contacts/42",
        headers: anything,
        body: nil
      ).and_return(json_response(204))

      expect(adapter.delete!(object_type: "contacts", crm_id: "42")).to be(true)
    end

    it "returns false on 404" do
      expect(http).to receive(:request).with(
        :delete, anything, headers: anything, body: nil
      ).and_return(json_response(404, {code: "document_not_found", message: "Not found"}))

      expect(adapter.delete!(object_type: "contacts", crm_id: "999")).to be(false)
    end

    it "raises on other errors" do
      expect(http).to receive(:request).with(
        :delete, anything, headers: anything, body: nil
      ).and_return(json_response(500, {message: "Internal error"}))

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "42")
      end.to raise_error(Etlify::ApiError, /Internal error/)
    end
  end

  # ------------------------------------------------------------------ #
  # Error mapping
  # ------------------------------------------------------------------ #

  describe "error mapping" do
    it "raises Unauthorized on 401" do
      expect(http).to receive(:request).and_return(
        json_response(401, {message: "Key not found", code: "unauthorized"})
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1")
      end.to raise_error(Etlify::Unauthorized, /Key not found/)
    end

    it "raises RateLimited on 429" do
      expect(http).to receive(:request).and_return(
        json_response(429, {message: "Too many requests"})
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1")
      end.to raise_error(Etlify::RateLimited, /Too many requests/)
    end

    it "raises ValidationFailed on 422" do
      expect(http).to receive(:request).and_return(
        json_response(422, {message: "Invalid value", code: "invalid_parameter"})
      )

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1")
      end.to raise_error(Etlify::ValidationFailed)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(StandardError.new("network oops"))

      expect do
        adapter.delete!(object_type: "contacts", crm_id: "1")
      end.to raise_error(Etlify::TransportError, /network oops/)
    end
  end

  # ------------------------------------------------------------------ #
  # Argument validation
  # ------------------------------------------------------------------ #

  describe "argument validation" do
    it "raises on unsupported object_type" do
      expect do
        adapter.upsert!(object_type: "unknown", payload: {})
      end.to raise_error(ArgumentError, /Unsupported object_type/)
    end

    it "raises on missing api_key" do
      expect do
        described_class.new(api_key: "")
      end.to raise_error(ArgumentError, /api_key/)
    end

    it "raises on missing crm_id for delete" do
      expect do
        adapter.delete!(object_type: "contacts", crm_id: nil)
      end.to raise_error(ArgumentError, /crm_id/)
    end
  end

  # ------------------------------------------------------------------ #
  # Rate limiter
  # ------------------------------------------------------------------ #

  describe "rate limiter support" do
    it "delegates rate_limiter to client" do
      limiter = Etlify::RateLimiter.new(max_requests: 100, period: 3600)
      adapter.rate_limiter = limiter

      expect(adapter.rate_limiter).to eq(limiter)
    end
  end
end
