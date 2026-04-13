require "rails_helper"
require "etlify/adapters/airtable_v0/client"

RSpec.describe Etlify::Adapters::AirtableV0::Client do
  let(:token)   { "pat-test-token" }
  let(:base_id) { "appTEST123456" }
  let(:http)    { instance_double("HttpClient") }

  subject(:client) do
    described_class.new(
      access_token: token,
      base_id: base_id,
      http: http
    )
  end

  describe "#base_path" do
    it "returns the base path for a table ID" do
      expect(client.base_path("tblContacts"))
        .to eq("/appTEST123456/tblContacts")
    end

    it "URL-encodes table names with spaces" do
      expect(client.base_path("My Contacts"))
        .to eq("/appTEST123456/My%20Contacts")
    end

    it "URL-encodes path traversal attempts" do
      path = client.base_path("../../etc/passwd")
      expect(path).not_to include("/../")
      expect(path).to include("%2F")
    end
  end

  describe "#record_path" do
    it "returns the path for a specific record" do
      expect(client.record_path("tblX", "recABC"))
        .to eq("/appTEST123456/tblX/recABC")
    end

    it "URL-encodes the record ID" do
      path = client.record_path("tblX", "../../secret")
      expect(path).not_to include("/../")
    end
  end

  describe "#raise_for_error!" do
    it "does nothing on 2xx status" do
      response = {status: 200, json: {}, body: "{}"}
      expect { client.raise_for_error!(response, path: "/test") }
        .not_to raise_error
    end

    it "raises Unauthorized on 401" do
      response = {
        status: 401,
        json: {
          "error" => {
            "type" => "AUTHENTICATION_REQUIRED",
            "message" => "No auth",
          },
        },
        body: "{}",
      }
      expect { client.raise_for_error!(response, path: "/test") }
        .to raise_error(Etlify::Unauthorized, /No auth/)
    end

    it "raises Unauthorized on 403" do
      response = {
        status: 403,
        json: {
          "error" => {
            "type" => "FORBIDDEN",
            "message" => "Access denied",
          },
        },
        body: "{}",
      }
      expect { client.raise_for_error!(response, path: "/test") }
        .to raise_error(Etlify::Unauthorized, /Access denied/)
    end

    it "raises NotFound on 404" do
      response = {
        status: 404,
        json: {"error" => {"type" => "NOT_FOUND", "message" => "Gone"}},
        body: "{}",
      }
      expect { client.raise_for_error!(response, path: "/test") }
        .to raise_error(Etlify::NotFound)
    end

    it "raises ValidationFailed on 422" do
      response = {
        status: 422,
        json: {
          "error" => {
            "type" => "INVALID_REQUEST",
            "message" => "Bad field",
          },
        },
        body: "{}",
      }
      expect { client.raise_for_error!(response, path: "/test") }
        .to raise_error(Etlify::ValidationFailed)
    end

    it "raises RateLimited on 429" do
      response = {
        status: 429,
        json: {
          "error" => {
            "type" => "RATE_LIMIT_REACHED",
            "message" => "Too many requests",
          },
        },
        body: "{}",
      }
      expect { client.raise_for_error!(response, path: "/test") }
        .to raise_error(Etlify::RateLimited)
    end

    it "raises ApiError on other status codes" do
      response = {
        status: 500,
        json: {
          "error" => {
            "type" => "SERVER_ERROR",
            "message" => "Server down",
          },
        },
        body: "{}",
      }
      expect { client.raise_for_error!(response, path: "/test") }
        .to raise_error(Etlify::ApiError, /Server down/)
    end
  end

  describe "transport errors" do
    it "wraps transport exceptions into TransportError" do
      allow(http).to receive(:request)
        .and_raise(StandardError.new("connection reset"))

      expect { client.get("/test") }
        .to raise_error(
          Etlify::TransportError, /connection reset/
        )
    end
  end

  describe "rate limiter integration" do
    it "calls throttle! before each request" do
      limiter = instance_double(
        Etlify::RateLimiter, throttle!: nil
      )
      client.rate_limiter = limiter

      allow(http).to receive(:request).and_return(
        {status: 200, body: "{}"}
      )

      client.get("/test")
      expect(limiter).to have_received(:throttle!).once
    end
  end
end
