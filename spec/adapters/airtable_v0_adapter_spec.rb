require "rails_helper"
require "json"

RSpec.describe Etlify::Adapters::AirtableV0Adapter do
  let(:api_key) { "key_test" }
  let(:base_id) { "app123" }
  let(:table)   { "Contacts" }

  # Minimal fake HTTP client with a queue of canned responses
  let(:http) do
    Class.new do
      attr_reader :calls
      def initialize
        @calls = []
        @queue = []
        @next = nil
      end
      def request(method, url, headers:, body: nil)
        @calls << { method: method, url: url, headers: headers, body: body }
        return @queue.shift if @queue.any?
        return @next if @next
        { status: 200, body: "{}" }
      end
      def queue=(arr)
        @queue = arr.dup
      end
      def next=(resp)
        @next = resp
      end
    end.new
  end

  subject(:adapter) do
    described_class.new(api_key: api_key, base_id: base_id, table: table,
                        http_client: http)
  end

  describe "#upsert!" do
    it "creates when no match is found" do
      http.queue = [
        { status: 200, body: { records: [] }.to_json },
        { status: 200, body: { id: "recNEW" }.to_json }
      ]

      id = adapter.upsert!(
        object_type: "Contacts",
        payload: { email: "a@b.com", first_name: "A" },
        id_property: "email"
      )

      expect(id).to eq("recNEW")
      get_call, post_call = http.calls
      expect(get_call[:method]).to eq(:get)
      expect(get_call[:url]).to include("filterByFormula=")
      expect(post_call[:method]).to eq(:post)
      fields = JSON.parse(post_call[:body])["fields"]
      expect(fields).to eq({ "email" => "a@b.com", "first_name" => "A" })
    end

    it "updates when a match is found via id_property" do
      http.queue = [
        { status: 200, body: { records: [{ id: "recABC" }] }.to_json },
        { status: 200, body: { id: "recABC" }.to_json }
      ]

      id = adapter.upsert!(
        payload: { email: "a@b.com", first_name: "A" },
        id_property: :email
      )

      expect(id).to eq("recABC")
      _, patch_call = http.calls
      expect(patch_call[:method]).to eq(:patch)
      expect(patch_call[:url]).to include("/v0/app123/Contacts/recABC")
      expect(JSON.parse(patch_call[:body])["fields"]).to eq({
        "first_name" => "A"
      })
    end

    it "injects id_property back on create when removed for search" do
      http.queue = [
        { status: 200, body: { records: [] }.to_json },
        { status: 200, body: { id: "recZ" }.to_json }
      ]

      id = adapter.upsert!(
        payload: { email: "a@b.com", first_name: "A" },
        id_property: "email"
      )

      expect(id).to eq("recZ")
      post_call = http.calls.last
      fields = JSON.parse(post_call[:body])["fields"]
      expect(fields["email"]).to eq("a@b.com")
    end

    it "creates when id_property has no value in payload" do
      http.queue = [
        { status: 200, body: { id: "recN" }.to_json }
      ]

      id = adapter.upsert!(
        payload: { first_name: "A" },
        id_property: :email
      )

      expect(id).to eq("recN")
      post_call = http.calls.last
      fields = JSON.parse(post_call[:body])["fields"]
      expect(fields.key?("email")).to be(false)
    end

    it "accepts object_type override over default table" do
      http.queue = [
        { status: 200, body: { records: [] }.to_json },
        { status: 200, body: { id: "recNEW" }.to_json }
      ]

      adapter.upsert!(
        object_type: "Another",
        payload: { email: "x@x" },
        id_property: :email
      )

      expect(http.calls.first[:url]).to include("/v0/app123/Another?")
    end

    it "raises on non-Hash payload" do
      expect do
        adapter.upsert!(payload: "oops")
      end.to raise_error(ArgumentError)
    end

    it "raises Etlify errors on search failure (401)" do
      http.next = {
        status: 401,
        body: { error: { type: "AUTH", message: "no" } }.to_json
      }

      expect do
        adapter.upsert!(payload: { email: "x@y" }, id_property: :email)
      end.to raise_error(Etlify::Unauthorized)
    end

    it "raises Etlify::ApiError on unexpected status (500)" do
      http.next = {
        status: 500,
        body: { error: { type: "SERVER", message: "boom" } }.to_json
      }

      expect do
        adapter.upsert!(payload: { email: "x@y" }, id_property: :email)
      end.to raise_error(Etlify::ApiError)
    end

    it "wraps transport errors into Etlify::TransportError" do
      exploding = Class.new do
        def request(*) = raise IOError, "socket closed"
      end.new

      bad = described_class.new(api_key: api_key, base_id: base_id,
                                table: table, http_client: exploding)

      expect do
        bad.upsert!(payload: { email: "x@y" }, id_property: :email)
      end.to raise_error(Etlify::TransportError)
    end

    it "returns id as String even if JSON has non-string id" do
      http.queue = [
        { status: 200, body: { records: [] }.to_json },
        { status: 200, body: { id: 123 }.to_json }
      ]

      id = adapter.upsert!(
        payload: { email: "a@b.com" },
        id_property: :email
      )

      expect(id).to eq("123")
    end
  end

  describe "#delete!" do
    it "returns true on success" do
      http.next = { status: 200, body: { deleted: true }.to_json }
      expect(adapter.delete!(crm_id: "rec123")).to be(true)
    end

    it "returns false on 404" do
      http.next = { status: 404, body: {}.to_json }
      expect(adapter.delete!(crm_id: "rec404")).to be(false)
    end

    it "raises mapped error on 429" do
      http.next = {
        status: 429,
        body: { error: { type: "RATE", message: "slow" } }.to_json
      }

      expect do
        adapter.delete!(crm_id: "rec1")
      end.to raise_error(Etlify::RateLimited)
    end

    it "raises mapped error on 422" do
      http.next = { status: 422, body: { error: {} }.to_json }

      expect do
        adapter.delete!(crm_id: "rec1")
      end.to raise_error(Etlify::ValidationFailed)
    end

    it "raises ArgumentError when crm_id missing" do
      expect do
        adapter.delete!(crm_id: "")
      end.to raise_error(ArgumentError)
    end

    it "accepts object_type override" do
      http.next = { status: 200, body: { deleted: true }.to_json }
      adapter.delete!(object_type: "Companies", crm_id: "rec1")
      expect(http.calls.first[:url]).to include("/v0/app123/Companies/rec1")
    end
  end

  describe "table resolution" do
    it "raises when neither object_type nor default is set" do
      a = described_class.new(api_key: api_key, base_id: base_id,
                              table: nil, http_client: http)
      expect do
        a.upsert!(payload: {})
      end.to raise_error(ArgumentError)
    end
  end

  describe "#request (private)" do
    it "builds URL with query string and sets headers" do
      http.next = { status: 200, body: "{}" }
      resp = adapter.send(
        :request,
        :get,
        "/#{base_id}/#{table}",
        query: { filterByFormula: "{Email} = 'a'" }
      )

      expect(resp[:status]).to eq(200)
      call = http.calls.last
      expect(call[:url]).to include("?filterByFormula=")
      expect(call[:headers]["Authorization"]).to eq("Bearer #{api_key}")
      expect(call[:headers]["Accept"]).to eq("application/json")
    end

    it "serializes body as JSON when provided" do
      http.next = { status: 200, body: "{}" }
      adapter.send(
        :request,
        :post,
        "/#{base_id}/#{table}",
        body: { fields: { a: 1 } }
      )

      body = http.calls.last[:body]
      expect(body).to be_a(String)
      expect(JSON.parse(body)).to eq({ "fields" => { "a" => 1 } })
    end

    it "wraps transport error" do
      exploding = Class.new do
        def request(*) = raise IOError, "boom"
      end.new

      a = described_class.new(api_key: api_key, base_id: base_id,
                              table: table, http_client: exploding)

      expect do
        a.send(:request, :get, "/x")
      end.to raise_error(Etlify::TransportError)
    end
  end

  describe "#raise_for_error! (private)" do
    it "maps 401/403 to Unauthorized" do
      resp = { status: 401, json: { error: { type: "AUTH", message: "x" } } }
      expect do
        adapter.send(:raise_for_error!, resp, path: "/x")
      end.to raise_error(Etlify::Unauthorized)

      resp = { status: 403, json: { error: { message: "x" } } }
      expect do
        adapter.send(:raise_for_error!, resp, path: "/x")
      end.to raise_error(Etlify::Unauthorized)
    end

    it "maps 404 to NotFound" do
      resp = { status: 404, json: { error: { type: "NF" } } }
      expect do
        adapter.send(:raise_for_error!, resp, path: "/x")
      end.to raise_error(Etlify::NotFound)
    end

    it "maps 409/422 to ValidationFailed" do
      [409, 422].each do |s|
        resp = { status: s, json: { error: {} } }
        expect do
          adapter.send(:raise_for_error!, resp, path: "/x")
        end.to raise_error(Etlify::ValidationFailed)
      end
    end

    it "maps 429 to RateLimited" do
      resp = { status: 429, json: { error: {} } }
      expect do
        adapter.send(:raise_for_error!, resp, path: "/x")
      end.to raise_error(Etlify::RateLimited)
    end

    it "maps 500 to ApiError" do
      resp = { status: 500, json: { message: "boom" } }
      expect do
        adapter.send(:raise_for_error!, resp, path: "/x")
      end.to raise_error(Etlify::ApiError)
    end

    it "handles nil/invalid json payloads gracefully" do
      resp = { status: 500, json: nil }
      expect do
        adapter.send(:raise_for_error!, resp, path: "/x")
      end.to raise_error(Etlify::ApiError)
    end
  end

  describe "#find_record_id_by_field (private)" do
    it "returns nil on empty records" do
      http.next = { status: 200, body: { records: [] }.to_json }
      id = adapter.send(:find_record_id_by_field, "Contacts", "email", "a")
      expect(id).to be_nil
    end

    it "returns id when present" do
      http.next = { status: 200, body: { records: [{ id: "recX" }] }.to_json }
      id = adapter.send(:find_record_id_by_field, "Contacts", "email", "a")
      expect(id).to eq("recX")
    end

    it "returns nil on 404" do
      http.next = { status: 404, body: {}.to_json }
      id = adapter.send(:find_record_id_by_field, "Contacts", "email", "a")
      expect(id).to be_nil
    end

    it "raises on 500+" do
      http.next = { status: 500, body: { error: {} }.to_json }
      expect do
        adapter.send(:find_record_id_by_field, "Contacts", "email", "a")
      end.to raise_error(Etlify::ApiError)
    end

    it "sets filterByFormula, maxRecords and pageSize" do
      http.next = { status: 200, body: { records: [] }.to_json }
      adapter.send(:find_record_id_by_field, "Contacts", "email", "a")
      url = http.calls.last[:url]
      expect(url).to include("filterByFormula=")
      expect(url).to include("maxRecords=1")
      expect(url).to include("pageSize=1")
    end
  end

  describe "#update_record (private)" do
    it "returns true on success and stringifies keys" do
      http.next = { status: 200, body: { id: "rec1" }.to_json }
      ok = adapter.send(:update_record, "Contacts", "rec1", { a: 1 })
      expect(ok).to be(true)
      body = JSON.parse(http.calls.last[:body])
      expect(body).to eq({ "fields" => { "a" => 1 } })
    end

    it "raises mapped error on 422" do
      http.next = { status: 422, body: { error: {} }.to_json }
      expect do
        adapter.send(:update_record, "Contacts", "rec1", { a: 1 })
      end.to raise_error(Etlify::ValidationFailed)
    end
  end

  describe "#create_record (private)" do
    it "returns id on success" do
      http.next = { status: 200, body: { id: "rec99" }.to_json }
      id = adapter.send(:create_record, "Contacts", { a: 1 }, nil, nil)
      expect(id).to eq("rec99")
    end

    it "raises ApiError when 2xx without id" do
      # simulate 200 with no id and then ensure raise mapping occurs
      http.next = { status: 200, body: {}.to_json }
      expect do
        adapter.send(:create_record, "Contacts", { a: 1 }, nil, nil)
      end.to raise_error(Etlify::ApiError)
    end

    it "raises mapped error (403)" do
      http.next = { status: 403, body: { error: {} }.to_json }
      expect do
        adapter.send(:create_record, "Contacts", { a: 1 }, nil, nil)
      end.to raise_error(Etlify::Unauthorized)
    end

    it "injects id_property when missing in fields" do
      http.next = { status: 200, body: { id: "rec1" }.to_json }
      adapter.send(:create_record, "Contacts", { a: 1 }, :email, "a@b")
      body = JSON.parse(http.calls.last[:body])
      expect(body["fields"]).to include({ "email" => "a@b" })
    end
  end

  describe "helpers (private)" do
    it "builds equality formula for strings with quotes" do
      f = adapter.send(:build_equality_formula, "Name", "O'Hara")
      expect(f).to eq("{Name} = 'O\'Hara'")
    end

    it "builds equality formula for booleans" do
      expect(adapter.send(:build_equality_formula, "Active", true))
        .to eq("{Active} = TRUE()")
      expect(adapter.send(:build_equality_formula, "Active", false))
        .to eq("{Active} = FALSE()")
    end

    it "builds equality formula for numbers" do
      expect(adapter.send(:build_equality_formula, "Age", 12))
        .to eq("{Age} = 12")
    end

    it "builds equality formula for objects (JSON)" do
      f = adapter.send(:build_equality_formula, "Meta", { a: 1 })
      expect(f).to eq("{Meta} = '{\"a\":1}'")
    end

    it "escapes } in field name" do
      f = adapter.send(:build_equality_formula, "A}B", "x")
      expect(f).to start_with("{A)B}")
    end

    it "stringify_keys works" do
      out = adapter.send(:stringify_keys, { a: 1, "b" => 2 })
      expect(out).to eq({ "a" => 1, "b" => 2 })
    end

    it "parse_json_safe returns nil on invalid JSON and on empty" do
      expect(adapter.send(:parse_json_safe, "not json")).to be_nil
      expect(adapter.send(:parse_json_safe, "")).to be_nil
    end

    it "enc encodes special characters" do
      expect(adapter.send(:enc, "A B/C")).to eq("A+B%2FC")
    end
  end
end

# Cover DefaultHttp paths explicitly to hit lines inside that class
RSpec.describe Etlify::Adapters::AirtableV0Adapter::DefaultHttp do
  subject(:http) { described_class.new }

  it "raises on unsupported method" do
    expect do
      http.request(:put, "https://x", headers: {})
    end.to raise_error(ArgumentError, /Unsupported method/)
  end

  it "returns status and body on success (with Net::HTTP stubbed)" do
    # Build a fake Net::HTTP ecosystem
    response = Struct.new(:code, :body)
    fake_res = response.new("200", "{}")

    fake_req_class = Class.new do
      def initialize(path, headers)
        @path = path
        @headers = headers
      end
      attr_accessor :body
    end

    fake_http = Class.new do
      def initialize(*); end
      def use_ssl=(v); @ssl = v; end
      def request(req); @req = req; @res; end
      attr_writer :res
    end

    stub_const("Net::HTTP::Get", fake_req_class)

    instance = fake_http.new
    instance.res = fake_res

    allow(Net::HTTP).to receive(:new).and_return(instance)

    out = http.request(:get, "https://api.airtable.com/v0", headers: {})
    expect(out).to eq({ status: 200, body: "{}" })
  end

  it "re-raises transport errors (rescued then raised)" do
    allow(Net::HTTP).to receive(:new).and_raise(Errno::ECONNREFUSED)

    expect {
      http.request(:get, "https://x", headers: {})
    }.to raise_error(Errno::ECONNREFUSED)
  end
end
