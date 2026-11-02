require "rails_helper"
require "etlify/adapters/zoho_crm/access_token"

RSpec.describe Etlify::Adapters::ZohoCrm::AccessToken do
  let(:http)  { instance_double("HttpClient") }
  let(:cache) { nil }
  let(:token_url) { "https://accounts.zoho.eu/oauth/v2/token" }

  subject(:access_token) { build_access_token(cache: cache) }

  def build_access_token(cache:)
    described_class.new(
      accounts_url: "https://accounts.zoho.eu",
      client_id: "client-id",
      client_secret: "client-secret",
      refresh_token: "refresh-token",
      http: http,
      cache: cache
    )
  end

  def token_response(token, expires_in: 3600)
    {status: 200, body: {access_token: token, expires_in: expires_in, token_type: "Bearer"}.to_json}
  end

  describe "#value" do
    it "exchanges the refresh token with a form-encoded POST", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        token_url,
        headers: hash_including("Content-Type" => "application/x-www-form-urlencoded"),
        body: satisfy do |body|
          URI.decode_www_form(body).to_h == {
            "refresh_token" => "refresh-token",
            "client_id" => "client-id",
            "client_secret" => "client-secret",
            "grant_type" => "refresh_token",
          }
        end
      ).once.and_return(token_response("tok-1"))

      expect(access_token.value).to eq("tok-1")
      expect(access_token.value).to eq("tok-1")
    end

    it "refreshes once the token is within the expiry margin" do
      expect(http).to receive(:request).twice.and_return(token_response("tok-1"), token_response("tok-2"))

      Timecop.freeze(Time.now) do
        expect(access_token.value).to eq("tok-1")

        Timecop.travel(Time.now + 3600 - described_class::EXPIRY_MARGIN + 1)
        expect(access_token.value).to eq("tok-2")
      end
    end

    context "with a shared cache" do
      let(:cache) { ActiveSupport::Cache::MemoryStore.new }

      it "reuses the token another instance obtained" do
        expect(http).to receive(:request).once.and_return(token_response("tok-1"))

        expect(access_token.value).to eq("tok-1")
        expect(build_access_token(cache: cache).value).to eq("tok-1")
      end

      it "lets the process with the earliest offset refresh for the others" do
        expect(Random).to receive(:rand).with(0..described_class::MAX_REFRESH_OFFSET).and_return(0, 120)
        expect(http).to receive(:request).twice.and_return(token_response("tok-1"), token_response("tok-2"))
        late_process = build_access_token(cache: cache)
        early_process = build_access_token(cache: cache)

        Timecop.freeze(Time.now) do
          expect(late_process.value).to eq("tok-1")
          expect(early_process.value).to eq("tok-1")

          Timecop.travel(Time.now + 3600 - described_class::EXPIRY_MARGIN - 120 + 1)
          expect(early_process.value).to eq("tok-2")
          expect(late_process.value).to eq("tok-1")

          Timecop.travel(Time.now + 120)
          expect(late_process.value).to eq("tok-2")
        end
      end
    end

    context "when expires_in is missing or zero" do
      let(:cache) { ActiveSupport::Cache::MemoryStore.new }

      it "keeps the token for an hour, in memory and in the cache", :aggregate_failures do
        expect(http).to receive(:request).twice.and_return(
          {status: 200, body: {access_token: "tok-1"}.to_json},
          token_response("tok-2", expires_in: 0)
        )

        Timecop.freeze(Time.now) do
          expect(access_token.value).to eq("tok-1")
          expect(access_token.value).to eq("tok-1")
          expect(build_access_token(cache: cache).value).to eq("tok-1")

          Timecop.travel(Time.now + 3600 - described_class::EXPIRY_MARGIN + 1)
          expect(access_token.value).to eq("tok-2")
          expect(build_access_token(cache: cache).value).to eq("tok-2")
        end
      end
    end

    context "when expires_in is shorter than an hour" do
      let(:cache) { ActiveSupport::Cache::MemoryStore.new }

      before { allow(Random).to receive(:rand).with(0..described_class::MAX_REFRESH_OFFSET).and_return(0) }

      it "honors the announced lifetime, minus the margin", :aggregate_failures do
        expect(http).to receive(:request).twice.and_return(
          token_response("tok-1", expires_in: 1800),
          token_response("tok-2", expires_in: 1800)
        )

        Timecop.freeze(Time.now) do
          expect(access_token.value).to eq("tok-1")

          Timecop.travel(Time.now + 1800 - described_class::EXPIRY_MARGIN - 1)
          expect(access_token.value).to eq("tok-1")
          expect(build_access_token(cache: cache).value).to eq("tok-1")

          Timecop.travel(Time.now + 2)
          expect(access_token.value).to eq("tok-2")
          expect(build_access_token(cache: cache).value).to eq("tok-2")
        end
      end

      it "keeps half the lifetime when it is shorter than the margin", :aggregate_failures do
        expect(http).to receive(:request).twice.and_return(
          token_response("tok-1", expires_in: 120),
          token_response("tok-2", expires_in: 120)
        )

        Timecop.freeze(Time.now) do
          expect(access_token.value).to eq("tok-1")

          Timecop.travel(Time.now + 59)
          expect(access_token.value).to eq("tok-1")
          expect(build_access_token(cache: cache).value).to eq("tok-1")

          Timecop.travel(Time.now + 2)
          expect(access_token.value).to eq("tok-2")
        end
      end
    end

    it "raises Unauthorized when Zoho rejects the refresh token with a 200" do
      expect(http).to receive(:request).and_return({status: 200, body: {error: "invalid_code"}.to_json})

      expect { access_token.value }.to raise_error(Etlify::Unauthorized, /error=invalid_code/)
    end

    it "raises RateLimited when the refresh quota is exceeded" do
      expect(http).to receive(:request).and_return({status: 200, body: {error: "Access Denied"}.to_json})

      expect { access_token.value }.to raise_error(Etlify::RateLimited, /error=Access Denied/) { |error|
        expect(error.retry_after).to eq(60)
      }
    end

    it "raises Unauthorized on a 4xx without JSON body" do
      expect(http).to receive(:request).and_return({status: 400, body: "<html>Access Denied</html>"})

      expect { access_token.value }.to raise_error(Etlify::Unauthorized, /status=400/)
    end

    it "raises RateLimited on 429" do
      expect(http).to receive(:request).and_return({status: 429, body: ""})

      expect { access_token.value }.to raise_error(Etlify::RateLimited) { |error|
        expect(error.retry_after).to eq(60)
      }
    end

    it "raises ApiError on 5xx" do
      expect(http).to receive(:request).and_return({status: 503, body: ""})

      expect { access_token.value }.to raise_error(Etlify::ApiError) { |error|
        expect(error).not_to be_a(Etlify::Unauthorized)
      }
    end

    it "wraps transport errors" do
      expect(http).to receive(:request).and_raise(Timeout::Error, "timed out")

      expect { access_token.value }.to raise_error(Etlify::TransportError, /Timeout::Error: timed out/)
    end
  end

  describe "#inspect" do
    it "does not reveal the secret, the refresh token or the current token", :aggregate_failures do
      expect(http).to receive(:request).and_return(token_response("tok-1"))
      access_token.value

      expect(access_token.inspect).to include("client-id")
      expect(access_token.inspect).not_to include("client-secret", "refresh-token", "tok-1")
    end
  end

  describe "#invalidate!" do
    let(:cache) { ActiveSupport::Cache::MemoryStore.new }

    it "drops the rejected token from memory and from the cache" do
      expect(http).to receive(:request).twice.and_return(token_response("tok-1"), token_response("tok-2"))

      expect(access_token.value).to eq("tok-1")
      access_token.invalidate!("tok-1")

      expect(access_token.value).to eq("tok-2")
      expect(build_access_token(cache: cache).value).to eq("tok-2")
    end

    it "keeps a fresher token written by another process" do
      expect(http).to receive(:request).twice.and_return(token_response("tok-1"), token_response("tok-2"))

      expect(access_token.value).to eq("tok-1")
      other_process = build_access_token(cache: cache)
      other_process.invalidate!("tok-1")
      expect(other_process.value).to eq("tok-2")

      access_token.invalidate!("tok-1")

      expect(access_token.value).to eq("tok-2")
    end
  end
end
