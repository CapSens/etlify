require "rails_helper"
require "etlify/adapters/zoho_crm_adapter"

RSpec.describe Etlify::Adapters::ZohoCrmAdapter do
  let(:http) { instance_double("HttpClient") }
  let(:api)  { "https://www.zohoapis.com/crm/v8" }
  let(:auth) { {"Authorization" => "Zoho-oauthtoken tok-1"} }

  subject(:adapter) do
    described_class.new(
      client_id: "client-id",
      client_secret: "client-secret",
      refresh_token: "refresh-token",
      token_cache: false,
      http_client: http
    )
  end

  # Token issuance is covered by ZohoCrm::AccessToken's own spec.
  before do
    allow(http).to receive(:request).with(
      :post,
      "https://accounts.zoho.com/oauth/v2/token",
      headers: anything,
      body: anything
    ).and_return({status: 200, body: {access_token: "tok-1", expires_in: 3600}.to_json})
  end

  def zoho_success(id, action: "insert")
    {code: "SUCCESS", action: action, details: {id: id}, message: "record added", status: "success"}
  end

  def zoho_error(code, details = {})
    {code: code, details: details, message: "invalid data", status: "error"}
  end

  describe "#initialize" do
    it "shares the access token through Etlify.config.cache_store by default" do
      build = lambda do
        described_class.new(client_id: "client-id", client_secret: "client-secret", refresh_token: "refresh-token",
          http_client: http)
      end

      expect(http).to receive(:request).with(:post, "https://accounts.zoho.com/oauth/v2/token", headers: anything,
        body: anything).once
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts/42", headers: hash_including(auth), body: nil)
        .twice.and_return({status: 200, body: {data: [zoho_success("42")]}.to_json})

      build.call.delete!(object_type: "Contacts", crm_id: "42")
      build.call.delete!(object_type: "Contacts", crm_id: "42")
    end

    it "targets the data center's API and Accounts servers" do
      eu_adapter = described_class.new(
        client_id: "client-id",
        client_secret: "client-secret",
        refresh_token: "refresh-token",
        data_center: :eu,
        token_cache: false,
        http_client: http
      )

      expect(http).to receive(:request).with(
        :post,
        "https://accounts.zoho.eu/oauth/v2/token",
        headers: anything,
        body: anything
      ).and_return({status: 200, body: {access_token: "tok-eu", expires_in: 3600}.to_json})
      expect(http).to receive(:request).with(
        :delete,
        "https://www.zohoapis.eu/crm/v8/Contacts/42",
        headers: hash_including("Authorization" => "Zoho-oauthtoken tok-eu"),
        body: nil
      ).and_return({status: 200, body: {data: [zoho_success("42")]}.to_json})

      eu_adapter.delete!(object_type: "Contacts", crm_id: "42")
    end

    it "honors a custom api_version" do
      v7_adapter = described_class.new(
        client_id: "client-id",
        client_secret: "client-secret",
        refresh_token: "refresh-token",
        api_version: "v7",
        token_cache: false,
        http_client: http
      )

      expect(http).to receive(:request).with(
        :delete,
        "https://www.zohoapis.com/crm/v7/Contacts/42",
        headers: anything,
        body: nil
      ).and_return({status: 200, body: {data: [zoho_success("42")]}.to_json})

      v7_adapter.delete!(object_type: "Contacts", crm_id: "42")
    end

    it "raises on an unknown data center" do
      expect do
        described_class.new(client_id: "a", client_secret: "b", refresh_token: "c", data_center: :mars)
      end.to raise_error(ArgumentError, /data_center must be one of/)
    end

    it "raises on blank credentials" do
      expect do
        described_class.new(client_id: "a", client_secret: "", refresh_token: "c")
      end.to raise_error(ArgumentError, /client_secret/)
    end
  end

  describe "#inspect" do
    it "does not reveal credentials, even once a token is held", :aggregate_failures do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts/100", headers: anything, body: nil)
        .and_return({status: 200, body: {data: [zoho_success("100")]}.to_json})
      adapter.delete!(object_type: "Contacts", crm_id: "100")

      expect(adapter.inspect).to include("https://www.zohoapis.com/crm/v8")
      expect(adapter.inspect).not_to include("client-secret", "refresh-token", "tok-1")
    end
  end

  describe "#upsert!" do
    it "upserts on the match field when crm_id is unknown", :aggregate_failures do
      expect(http).to receive(:request).with(
        :post,
        "#{api}/Contacts/upsert",
        headers: hash_including(auth),
        body: satisfy do |body|
          JSON.parse(body) == {
            "data" => [{"Last_Name" => "Doe", "Email" => "john@example.com"}],
            "duplicate_check_fields" => ["Email"],
          }
        end
      ).and_return({status: 201, body: {data: [zoho_success("100")]}.to_json})

      id = adapter.upsert!(
        object_type: "Contacts",
        payload: {Last_Name: "Doe"},
        match_property: :Email,
        match_value: " john@example.com "
      )

      expect(id).to eq("100")
    end

    it "keeps the payload's own value of the match field" do
      expect(http).to receive(:request).with(
        :post,
        "#{api}/Contacts/upsert",
        headers: anything,
        body: satisfy { |body| JSON.parse(body)["data"] == [{"Email" => "Primary@example.com"}] }
      ).and_return({status: 200, body: {data: [zoho_success("100", action: "update")]}.to_json})

      adapter.upsert!(
        object_type: "Contacts",
        payload: {"Email" => "Primary@example.com"},
        match_property: "Email",
        match_value: "primary@example.com"
      )
    end

    it "updates by crm_id without matching, sending the payload as-is", :aggregate_failures do
      expect(http).to receive(:request).with(
        :put,
        "#{api}/Contacts/100",
        headers: hash_including(auth),
        body: satisfy { |body| JSON.parse(body) == {"data" => [{"Last_Name" => "Doe"}]} }
      ).and_return({status: 200, body: {data: [zoho_success("100", action: "update")]}.to_json})

      id = adapter.upsert!(
        object_type: "Contacts",
        payload: {Last_Name: "Doe"},
        match_property: "Email",
        match_value: "",
        crm_id: 100
      )

      expect(id).to eq("100")
    end

    it "raises when neither crm_id nor match_value is given" do
      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "  ")
      end.to raise_error(ArgumentError, /match_value/)
    end

    it "validates its arguments", :aggregate_failures do
      expect do
        adapter.upsert!(object_type: "", payload: {}, match_property: "Email", match_value: "x")
      end.to raise_error(ArgumentError, /object_type/)
      expect do
        adapter.upsert!(object_type: "Contacts", payload: nil, match_property: "Email", match_value: "x")
      end.to raise_error(ArgumentError, /payload/)
      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: nil, match_value: "x")
      end.to raise_error(ArgumentError, /match_property/)
    end

    it "raises ValidationFailed on a record error behind a 400", :aggregate_failures do
      expect(http).to receive(:request).with(:post, "#{api}/Contacts/upsert", headers: anything, body: anything)
        .and_return(
          {
            status: 400,
            body: {data: [zoho_error("MANDATORY_NOT_FOUND", {api_name: "Last_Name"})]}.to_json,
          }
        )

      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "a@b.c")
      end.to raise_error(Etlify::ValidationFailed) { |error|
        expect(error.status).to eq(400)
        expect(error.code).to eq("MANDATORY_NOT_FOUND")
        expect(error.details).to eq({"api_name" => "Last_Name"})
        expect(error.message).to include("code=MANDATORY_NOT_FOUND", '"api_name":"Last_Name"')
      }
    end

    it "raises ValidationFailed on a record error behind a 2xx" do
      expect(http).to receive(:request).with(:put, "#{api}/Contacts/100", headers: anything, body: anything)
        .and_return({status: 202, body: {data: [zoho_error("INVALID_DATA")]}.to_json})

      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "", crm_id: "100")
      end.to raise_error(Etlify::ValidationFailed, /code=INVALID_DATA/)
    end

    it "raises ApiError when Zoho returns no record id" do
      expect(http).to receive(:request).with(:post, "#{api}/Contacts/upsert", headers: anything, body: anything)
        .and_return({status: 200, body: {data: []}.to_json})

      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "a@b.c")
      end.to raise_error(Etlify::ApiError, /returned no record id/)
    end

    it "maps request-wide failures by status", :aggregate_failures do
      {
        403 => Etlify::Unauthorized,
        404 => Etlify::NotFound,
        422 => Etlify::ValidationFailed,
        429 => Etlify::RateLimited,
        500 => Etlify::ApiError,
      }.each do |status, error_class|
        expect(http).to receive(:request).with(:put, "#{api}/Contacts/100", headers: anything, body: anything)
          .and_return({status: status, body: {code: "SOME_CODE", message: "boom", status: "error"}.to_json})

        expect do
          adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "", crm_id: "100")
        end.to raise_error(error_class, /boom \(status=#{status}, path=\/Contacts\/100, code=SOME_CODE\)/)
      end
    end

    it "raises ApiError on a non-JSON error body" do
      expect(http).to receive(:request).with(:put, "#{api}/Contacts/100", headers: anything, body: anything)
        .and_return({status: 502, body: "<html>Bad Gateway</html>"})

      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "", crm_id: "100")
      end.to raise_error(Etlify::ApiError, /Zoho CRM API request failed \(status=502/)
    end

    it "wraps transport errors" do
      expect(http).to receive(:request).with(:put, "#{api}/Contacts/100", headers: anything, body: anything)
        .and_raise(Errno::ECONNRESET)

      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "", crm_id: "100")
      end.to raise_error(Etlify::TransportError, /ECONNRESET/)
    end
  end

  describe "access token rejection" do
    it "refreshes the token and retries once on INVALID_TOKEN" do
      expect(http).to receive(:request).with(
        :post,
        "https://accounts.zoho.com/oauth/v2/token",
        headers: anything,
        body: anything
      ).twice.and_return(
        {status: 200, body: {access_token: "tok-1", expires_in: 3600}.to_json},
        {status: 200, body: {access_token: "tok-2", expires_in: 3600}.to_json}
      )
      expect(http).to receive(:request).with(
        :put,
        "#{api}/Contacts/100",
        headers: hash_including(auth),
        body: anything
      ).and_return({status: 401, body: {code: "INVALID_TOKEN", message: "invalid oauth token"}.to_json})
      expect(http).to receive(:request).with(
        :put,
        "#{api}/Contacts/100",
        headers: hash_including("Authorization" => "Zoho-oauthtoken tok-2"),
        body: anything
      ).and_return({status: 200, body: {data: [zoho_success("100")]}.to_json})

      adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "", crm_id: "100")
    end

    it "raises Unauthorized when the retry is rejected too" do
      expect(http).to receive(:request).with(:put, "#{api}/Contacts/100", headers: anything, body: anything)
        .twice.and_return({status: 401, body: {code: "INVALID_TOKEN", message: "invalid oauth token"}.to_json})

      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "", crm_id: "100")
      end.to raise_error(Etlify::Unauthorized, /code=INVALID_TOKEN/)
    end

    it "does not retry on other authorization errors" do
      expect(http).to receive(:request).with(:put, "#{api}/Contacts/100", headers: anything, body: anything)
        .once.and_return({status: 401, body: {code: "OAUTH_SCOPE_MISMATCH", message: "invalid scope"}.to_json})

      expect do
        adapter.upsert!(object_type: "Contacts", payload: {}, match_property: "Email", match_value: "", crm_id: "100")
      end.to raise_error(Etlify::Unauthorized, /OAUTH_SCOPE_MISMATCH/)
    end
  end

  describe "#delete!" do
    it "returns true once Zoho deleted the record" do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts/100", headers: hash_including(auth), body: nil)
        .and_return({status: 200, body: {data: [zoho_success("100")]}.to_json})

      expect(adapter.delete!(object_type: "Contacts", crm_id: "100")).to be(true)
    end

    it "returns false when the id matches no record" do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts/100", headers: anything, body: nil)
        .and_return({status: 400, body: {data: [zoho_error("INVALID_DATA", {id: "100"})]}.to_json})

      expect(adapter.delete!(object_type: "Contacts", crm_id: "100")).to be(false)
    end

    it "returns false on 404" do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts/100", headers: anything, body: nil)
        .and_return({status: 404, body: ""})

      expect(adapter.delete!(object_type: "Contacts", crm_id: "100")).to be(false)
    end

    it "raises on any other failure" do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts/100", headers: anything, body: nil)
        .and_return({status: 400, body: {data: [zoho_error("CANNOT_DELETE")]}.to_json})

      expect do
        adapter.delete!(object_type: "Contacts", crm_id: "100")
      end.to raise_error(Etlify::ValidationFailed, /CANNOT_DELETE/)
    end

    it "requires a crm_id" do
      expect do
        adapter.delete!(object_type: "Contacts", crm_id: " ")
      end.to raise_error(ArgumentError, /crm_id/)
    end
  end

  describe "#batch_upsert!" do
    it "maps each input value to the id Zoho returned at the same position" do
      expect(http).to receive(:request).with(
        :post,
        "#{api}/Contacts/upsert",
        headers: hash_including(auth),
        body: satisfy do |body|
          JSON.parse(body) == {
            "data" => [
              {"Last_Name" => "A", "Email" => "a@example.com"},
              {"Last_Name" => "B", "Email" => "b@example.com"},
            ],
            "duplicate_check_fields" => ["Email"],
          }
        end
      ).and_return({status: 200, body: {data: [zoho_success("1"), zoho_success("2", action: "update")]}.to_json})

      mapping = adapter.batch_upsert!(
        object_type: "Contacts",
        inputs: [
          {value: "a@example.com ", properties: {Last_Name: "A"}},
          {"value" => "b@example.com", "properties" => {"Last_Name" => "B"}},
        ],
        match_property: "Email"
      )

      expect(mapping).to eq({"a@example.com " => "1", "b@example.com" => "2"})
    end

    it "sends at most 100 records per call", :aggregate_failures do
      inputs = Array.new(150) { |index| {value: "u#{index}", properties: {}} }
      sent_refs = []

      expect(http).to receive(:request).with(:post, "#{api}/Investors/upsert", headers: anything, body: anything)
        .twice do |_method, _url, body:, **|
          data = JSON.parse(body)["data"]
          sent_refs << data.map { |record| record["Ref"] }
          {status: 200, body: {data: data.map { |record| zoho_success("id-#{record['Ref']}") }}.to_json}
        end

      mapping = adapter.batch_upsert!(object_type: "Investors", inputs: inputs, match_property: "Ref")

      expect(sent_refs).to eq([Array.new(100) { |index| "u#{index}" }, Array.new(50) { |index| "u#{index + 100}" }])
      expect(mapping).to eq(Array.new(150) { |index| ["u#{index}", "id-u#{index}"] }.to_h)
    end

    it "leaves out the records Zoho returned no id for" do
      expect(http).to receive(:request).with(:post, "#{api}/Contacts/upsert", headers: anything, body: anything)
        .and_return({status: 200, body: {data: [zoho_success("1")]}.to_json})

      mapping = adapter.batch_upsert!(
        object_type: "Contacts",
        inputs: [{value: "a@example.com", properties: {}}, {value: "b@example.com", properties: {}}],
        match_property: "Email"
      )

      expect(mapping).to eq({"a@example.com" => "1"})
    end

    it "raises ValidationFailed when one record fails" do
      expect(http).to receive(:request).with(:post, "#{api}/Contacts/upsert", headers: anything, body: anything)
        .and_return(
          {
            status: 207,
            body: {data: [zoho_success("1"), zoho_error("INVALID_DATA", {json_path: "$.data[1].Email"})]}.to_json,
          }
        )

      expect do
        adapter.batch_upsert!(
          object_type: "Contacts",
          inputs: [{value: "a@example.com", properties: {}}, {value: "nope", properties: {}}],
          match_property: "Email"
        )
      end.to raise_error(Etlify::ValidationFailed, /\$\.data\[1\]\.Email/)
    end

    it "rejects a blank input value" do
      expect do
        adapter.batch_upsert!(object_type: "Contacts", inputs: [{value: " ", properties: {}}], match_property: "Email")
      end.to raise_error(ArgumentError, /non-blank :value/)
    end

    it "rejects empty inputs" do
      expect do
        adapter.batch_upsert!(object_type: "Contacts", inputs: [], match_property: "Email")
      end.to raise_error(ArgumentError, /inputs/)
    end
  end

  describe "#batch_update!" do
    it "updates by id and returns an identity mapping" do
      expect(http).to receive(:request).with(
        :put,
        "#{api}/Contacts",
        headers: hash_including(auth),
        body: satisfy do |body|
          JSON.parse(body) == {
            "data" => [
              {"Last_Name" => "A", "id" => "1"},
              {"Last_Name" => "B", "id" => "2"},
            ],
          }
        end
      ).and_return({status: 200, body: {data: [zoho_success("1"), zoho_success("2")]}.to_json})

      mapping = adapter.batch_update!(
        object_type: "Contacts",
        records: [{crm_id: "1", properties: {Last_Name: "A"}}, {"crm_id" => 2, "properties" => {"Last_Name" => "B"}}]
      )

      expect(mapping).to eq({"1" => "1", "2" => "2"})
    end

    it "raises ValidationFailed when one record fails" do
      expect(http).to receive(:request).with(:put, "#{api}/Contacts", headers: anything, body: anything)
        .and_return({status: 207, body: {data: [zoho_success("1"), zoho_error("INVALID_DATA")]}.to_json})

      expect do
        adapter.batch_update!(object_type: "Contacts", records: [{crm_id: "1"}, {crm_id: "2"}])
      end.to raise_error(Etlify::ValidationFailed)
    end
  end

  describe "#batch_delete!" do
    it "deletes ids in slices of 100" do
      ids = Array.new(101) { |index| index.to_s }

      expect(http).to receive(:request).with(
        :delete,
        "#{api}/Contacts?ids=#{ids.first(100).join('%2C')}",
        headers: hash_including(auth),
        body: nil
      ).and_return({status: 200, body: {data: ids.first(100).map { |id| zoho_success(id) }}.to_json})
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts?ids=100", headers: anything, body: nil)
        .and_return({status: 200, body: {data: [zoho_success("100")]}.to_json})

      expect(adapter.batch_delete!(object_type: "Contacts", crm_ids: ids)).to be(true)
    end

    it "treats ids matching no record as deleted, even behind a 400" do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts?ids=1%2C2", headers: anything, body: nil)
        .and_return({status: 400, body: {data: [zoho_error("INVALID_DATA"), zoho_error("INVALID_DATA")]}.to_json})

      expect(adapter.batch_delete!(object_type: "Contacts", crm_ids: [1, 2])).to be(true)
    end

    it "raises on the failure that is not a missing record" do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts?ids=1%2C2", headers: anything, body: nil)
        .and_return({status: 207, body: {data: [zoho_error("INVALID_DATA"), zoho_error("CANNOT_DELETE")]}.to_json})

      expect do
        adapter.batch_delete!(object_type: "Contacts", crm_ids: ["1", "2"])
      end.to raise_error(Etlify::ValidationFailed, /CANNOT_DELETE/)
    end

    it "raises on a request-wide failure" do
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts?ids=1", headers: anything, body: nil)
        .and_return({status: 500, body: {code: "INTERNAL_ERROR", message: "boom"}.to_json})

      expect do
        adapter.batch_delete!(object_type: "Contacts", crm_ids: ["1"])
      end.to raise_error(Etlify::ApiError, /INTERNAL_ERROR/)
    end
  end

  describe "#rate_limiter" do
    it "throttles every CRM API call, but not the token refresh" do
      rate_limiter = instance_double(Etlify::RateLimiter)
      adapter.rate_limiter = rate_limiter

      expect(rate_limiter).to receive(:throttle!).once
      expect(http).to receive(:request).with(:delete, "#{api}/Contacts/1", headers: anything, body: nil)
        .and_return({status: 200, body: {data: [zoho_success("1")]}.to_json})

      adapter.delete!(object_type: "Contacts", crm_id: "1")
    end
  end
end
