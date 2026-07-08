require "rails_helper"
require "etlify/adapters/airtable_v0_adapter"

RSpec.describe Etlify::Adapters::AirtableV0Adapter do
  let(:token)   { "pat-test-token" }
  let(:base_id) { "appTEST123456" }
  let(:table)   { "tblContacts" }
  let(:http)    { instance_double("HttpClient") }

  subject(:adapter) do
    described_class.new(
      access_token: token,
      base_id: base_id,
      http_client: http
    )
  end

  describe "#initialize" do
    it "raises on blank access_token" do
      expect do
        described_class.new(access_token: "", base_id: base_id)
      end.to raise_error(ArgumentError, /access_token/)
    end

    it "raises on blank base_id" do
      expect do
        described_class.new(access_token: token, base_id: "")
      end.to raise_error(ArgumentError, /base_id/)
    end
  end

  describe "#upsert!" do
    context "when record exists (search by match_property)" do
      it "PATCHes the record and returns its id",
         :aggregate_failures do
        # 1) Search via filterByFormula
        expect(http).to receive(:request).with(
          :get,
          %r{airtable\.com/v0/#{base_id}/#{table}\?},
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: nil
        ).and_return(
          {
            status: 200,
            body: {
              records: [{"id" => "recABC123", "fields" => {}}],
            }.to_json,
          }
        )

        # 2) Update
        expect(http).to receive(:request).with(
          :patch,
          "https://api.airtable.com/v0/#{base_id}/#{table}/recABC123",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["fields"] == {
              "Email" => "john@example.com",
              "Name" => "John",
            }
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: table,
          payload: {Email: "john@example.com", Name: "John"},
          match_property: "Email",
          match_value: "john@example.com"
        )
        expect(id).to eq("recABC123")
      end

      it "PATCHes the payload as-is without injecting the matching field",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {
            status: 200,
            body: {
              records: [{"id" => "recNOINJ", "fields" => {}}],
            }.to_json,
          }
        )

        expect(http).to receive(:request).with(
          :patch,
          "https://api.airtable.com/v0/#{base_id}/#{table}/recNOINJ",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["fields"] == {"Name" => "John"} &&
              !json["fields"].key?("Email")
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: table,
          payload: {Name: "John"},
          match_property: "Email",
          match_value: "john@example.com"
        )
        expect(id).to eq("recNOINJ")
      end
    end

    context "when crm_id is provided" do
      it "skips search and PATCHes directly",
         :aggregate_failures do
        expect(http).not_to receive(:request).with(
          :get, /filterByFormula/, anything
        )

        expect(http).to receive(:request).with(
          :patch,
          "https://api.airtable.com/v0/#{base_id}/#{table}/recDIRECT",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["fields"].is_a?(Hash) &&
              json["fields"]["Name"] == "John"
          end
        ).and_return({status: 200, body: "{}"})

        id = adapter.upsert!(
          object_type: table,
          payload: {Email: "john@example.com", Name: "John"},
          match_property: "Email",
          match_value: nil,
          crm_id: "recDIRECT"
        )
        expect(id).to eq("recDIRECT")
      end
    end

    context "when record does not exist" do
      it "POSTs a new record and returns its id",
         :aggregate_failures do
        # 1) Search -> no results
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        # 2) Create: payload already carries the match field, sent as-is
        expect(http).to receive(:request).with(
          :post,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["fields"] == {
              "Email" => "new@example.com",
              "Name" => "New",
            }
          end
        ).and_return(
          {status: 200, body: {id: "recNEW001"}.to_json}
        )

        id = adapter.upsert!(
          object_type: table,
          payload: {Email: "new@example.com", Name: "New"},
          match_property: "Email",
          match_value: "new@example.com"
        )
        expect(id).to eq("recNEW001")
      end

      it "injects the matching field when absent from payload",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        expect(http).to receive(:request).with(
          :post,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["fields"] == {
              "Name" => "New",
              "Email" => "new@example.com",
            }
          end
        ).and_return(
          {status: 200, body: {id: "recINJ001"}.to_json}
        )

        id = adapter.upsert!(
          object_type: table,
          payload: {Name: "New"},
          match_property: "Email",
          match_value: "new@example.com"
        )
        expect(id).to eq("recINJ001")
      end

      it "keeps the payload value when it already carries the match field",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        expect(http).to receive(:request).with(
          :post,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["fields"] == {
              "Email" => "payload@example.com",
              "Name" => "P",
            }
          end
        ).and_return(
          {status: 200, body: {id: "recWIN001"}.to_json}
        )

        id = adapter.upsert!(
          object_type: table,
          payload: {Email: "payload@example.com", Name: "P"},
          match_property: "Email",
          match_value: "param@example.com"
        )
        expect(id).to eq("recWIN001")
      end

      it "keeps the payload value with a string-keyed match field",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        expect(http).to receive(:request).with(
          :post,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["fields"] == {
              "Email" => "payload@example.com",
              "Name" => "P",
            }
          end
        ).and_return(
          {status: 200, body: {id: "recWIN002"}.to_json}
        )

        id = adapter.upsert!(
          object_type: table,
          payload: {"Email" => "payload@example.com", "Name" => "P"},
          match_property: "Email",
          match_value: "param@example.com"
        )
        expect(id).to eq("recWIN002")
      end
    end

    context "when match_value is blank and crm_id is unknown" do
      it "raises ArgumentError without any HTTP call",
         :aggregate_failures do
        expect(http).not_to receive(:request)

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Name: "X"},
            match_property: "Email",
            match_value: nil
          )
        end.to raise_error(ArgumentError, /match_value/)

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Name: "X"},
            match_property: "Email",
            match_value: "   "
          )
        end.to raise_error(ArgumentError, /match_value/)
      end
    end

    it "accepts string or symbol keys in payload",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :get, /filterByFormula/, anything
      ).and_return(
        {status: 200, body: {records: []}.to_json}
      )

      expect(http).to receive(:request).with(
        :post,
        "https://api.airtable.com/v0/#{base_id}/#{table}",
        headers: hash_including(
          "Authorization" => "Bearer #{token}"
        ),
        body: satisfy do |body|
          json = JSON.parse(body)
          json["fields"] == {
            "Email" => "a@b.com",
            "Name" => "A",
          }
        end
      ).and_return(
        {status: 200, body: {id: "recMIX001"}.to_json}
      )

      id = adapter.upsert!(
        object_type: table,
        payload: {"Email" => "a@b.com", :Name => "A"},
        match_property: "Email",
        match_value: "a@b.com"
      )
      expect(id).to eq("recMIX001")
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.upsert!(
          object_type: "",
          payload: {},
          match_property: "Email",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.upsert!(
          object_type: table,
          payload: "not a hash",
          match_property: "Email",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError, /payload/)

      expect do
        adapter.upsert!(
          object_type: table,
          payload: {},
          match_property: "",
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError, /match_property/)

      expect do
        adapter.upsert!(
          object_type: table,
          payload: {},
          match_property: nil,
          match_value: "a@b.com"
        )
      end.to raise_error(ArgumentError, /match_property/)
    end

    it "escapes double quotes in formula values" do
      expect(http).to receive(:request).with(
        :get,
        satisfy do |url|
          decoded = URI.decode_www_form_component(url)
          decoded.include?('{Name} = "value with \\"quotes\\""')
        end,
        anything
      ).and_return(
        {status: 200, body: {records: []}.to_json}
      )

      expect(http).to receive(:request).with(
        :post, anything, anything
      ).and_return(
        {status: 200, body: {id: "recESC"}.to_json}
      )

      adapter.upsert!(
        object_type: table,
        payload: {Name: 'value with "quotes"'},
        match_property: "Name",
        match_value: 'value with "quotes"'
      )
    end

    it "stringifies numeric match_value (quoted in formula)" do
      expect(http).to receive(:request).with(
        :get,
        satisfy do |url|
          decoded = URI.decode_www_form_component(url)
          decoded.include?('{Score} = "42"')
        end,
        anything
      ).and_return(
        {status: 200, body: {records: []}.to_json}
      )

      expect(http).to receive(:request).with(
        :post, anything, anything
      ).and_return(
        {status: 200, body: {id: "recNUM"}.to_json}
      )

      adapter.upsert!(
        object_type: table,
        payload: {Score: 42, Name: "Test"},
        match_property: "Score",
        match_value: 42
      )
    end

    context "when search returns 404" do
      it "treats as not found and creates",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return({status: 404, body: ""})

        expect(http).to receive(:request).with(
          :post,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          anything
        ).and_return(
          {status: 200, body: {id: "rec404C"}.to_json}
        )

        id = adapter.upsert!(
          object_type: table,
          payload: {Email: "j@e.com", Name: "J"},
          match_property: "Email",
          match_value: "j@e.com"
        )
        expect(id).to eq("rec404C")
      end
    end

    context "when search returns 401" do
      it "raises Unauthorized" do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {
            status: 401,
            body: {
              error: {
                type: "AUTHENTICATION_REQUIRED",
                message: "Invalid token",
              },
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "x@y.com"},
            match_property: "Email",
            match_value: "x@y.com"
          )
        end.to raise_error(
          Etlify::Unauthorized, /Invalid token.*status=401/
        )
      end
    end

    context "when update returns 422" do
      it "raises ValidationFailed", :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {
            status: 200,
            body: {
              records: [{"id" => "rec9"}],
            }.to_json,
          }
        )

        expect(http).to receive(:request).with(
          :patch, /rec9/, anything
        ).and_return(
          {
            status: 422,
            body: {
              error: {
                type: "INVALID_REQUEST_UNKNOWN",
                message: "Invalid fields",
              },
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "john@example.com"},
            match_property: "Email",
            match_value: "john@example.com"
          )
        end.to raise_error(
          Etlify::ValidationFailed, /Invalid fields/
        )
      end
    end

    context "when create returns 429" do
      it "raises RateLimited", :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        expect(http).to receive(:request).with(
          :post, anything, anything
        ).and_return(
          {
            status: 429,
            body: {
              error: {
                type: "RATE_LIMIT_REACHED",
                message: "Too many requests",
              },
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "a@b.com"},
            match_property: "Email",
            match_value: "a@b.com"
          )
        end.to raise_error(
          Etlify::RateLimited, /Too many requests/
        )
      end
    end

    context "when search returns 500" do
      it "raises ApiError" do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {
            status: 500,
            body: {
              error: {
                type: "SERVER_ERROR",
                message: "Internal error",
              },
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "x@y.com"},
            match_property: "Email",
            match_value: "x@y.com"
          )
        end.to raise_error(
          Etlify::ApiError, /Internal error.*status=500/
        )
      end
    end

    context "when body is non-JSON" do
      it "raises ApiError with generic message" do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 500, body: "<html>oops</html>"}
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "x@y.com"},
            match_property: "Email",
            match_value: "x@y.com"
          )
        end.to raise_error(
          Etlify::ApiError,
          /Airtable API request failed/
        )
      end
    end

    context "when transport layer fails" do
      it "wraps into TransportError" do
        expect(http).to receive(:request).and_raise(
          StandardError.new("tcp reset")
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "x@y.com"},
            match_property: "Email",
            match_value: "x@y.com"
          )
        end.to raise_error(
          Etlify::TransportError, /tcp reset/
        )
      end
    end

    it "wraps transport errors during update",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :get, /filterByFormula/, anything
      ).and_return(
        {
          status: 200,
          body: {
            records: [{"id" => "rec1"}],
          }.to_json,
        }
      )

      expect(http).to receive(:request).with(
        :patch, /rec1/, anything
      ).and_raise(StandardError.new("network down"))

      expect do
        adapter.upsert!(
          object_type: table,
          payload: {Email: "x@y.com", Name: "X"},
          match_property: "Email",
          match_value: "x@y.com"
        )
      end.to raise_error(
        Etlify::TransportError, /network down/
      )
    end
  end

  describe "#delete!" do
    it "returns true on 2xx response" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.airtable.com/v0/#{base_id}/#{table}/recDEL1",
        headers: hash_including(
          "Authorization" => "Bearer #{token}"
        ),
        body: nil
      ).and_return({status: 200, body: "{}"})

      expect(
        adapter.delete!(object_type: table, crm_id: "recDEL1")
      ).to be true
    end

    it "returns false on 404 response" do
      expect(http).to receive(:request).with(
        :delete,
        "https://api.airtable.com/v0/#{base_id}/#{table}/recGONE",
        headers: hash_including(
          "Authorization" => "Bearer #{token}"
        ),
        body: nil
      ).and_return({status: 404, body: ""})

      expect(
        adapter.delete!(
          object_type: table, crm_id: "recGONE"
        )
      ).to be false
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.delete!(object_type: "", crm_id: "rec1")
      end.to raise_error(ArgumentError)

      expect do
        adapter.delete!(object_type: table, crm_id: nil)
      end.to raise_error(ArgumentError)

      expect do
        adapter.delete!(object_type: table, crm_id: "")
      end.to raise_error(ArgumentError)
    end

    it "raises ApiError on 400" do
      expect(http).to receive(:request).with(
        :delete, anything, anything
      ).and_return(
        {
          status: 400,
          body: {
            error: {
              type: "INVALID_REQUEST",
              message: "Bad request",
            },
          }.to_json,
        }
      )

      expect do
        adapter.delete!(object_type: table, crm_id: "rec1")
      end.to raise_error(Etlify::ApiError, /Bad request/)
    end

    it "raises Unauthorized on 401" do
      expect(http).to receive(:request).with(
        :delete, anything, anything
      ).and_return(
        {
          status: 401,
          body: {
            error: {
              type: "AUTHENTICATION_REQUIRED",
              message: "No auth",
            },
          }.to_json,
        }
      )

      expect do
        adapter.delete!(object_type: table, crm_id: "rec1")
      end.to raise_error(Etlify::Unauthorized)
    end

    it "raises ApiError on 500" do
      expect(http).to receive(:request).with(
        :delete, anything, anything
      ).and_return(
        {
          status: 500,
          body: {
            error: {
              type: "SERVER_ERROR",
              message: "Server down",
            },
          }.to_json,
        }
      )

      expect do
        adapter.delete!(object_type: table, crm_id: "rec1")
      end.to raise_error(Etlify::ApiError, /Server down/)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(
        StandardError.new("network oops")
      )

      expect do
        adapter.delete!(object_type: table, crm_id: "rec1")
      end.to raise_error(
        Etlify::TransportError, /network oops/
      )
    end
  end

  describe "#batch_upsert!" do
    context "with a single batch (<= 10 records)" do
      it "sends one PATCH with performUpsert and returns records",
         :aggregate_failures do
        inputs = [
          {value: "a@b.com", properties: {Name: "A"}},
          {value: "c@d.com", properties: {Name: "C"}},
        ]

        expect(http).to receive(:request).with(
          :patch,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            json = JSON.parse(body)
            json["performUpsert"]["fieldsToMergeOn"] == ["Email"] &&
              json["records"].size == 2 &&
              json["records"][0]["fields"] == {
                "Name" => "A",
                "Email" => "a@b.com",
              } &&
              json["records"][1]["fields"] == {
                "Name" => "C",
                "Email" => "c@d.com",
              }
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: [
                {
                  "id" => "recA",
                  "createdTime" => "2024-01-01",
                  "fields" => {"Email" => "a@b.com"},
                },
                {
                  "id" => "recC",
                  "createdTime" => "2024-01-01",
                  "fields" => {"Email" => "c@d.com"},
                },
              ],
            }.to_json,
          }
        )

        result = adapter.batch_upsert!(
          object_type: table,
          inputs: inputs,
          match_property: "Email"
        )
        expect(result).to eq(
          "a@b.com" => "recA",
          "c@d.com" => "recC"
        )
      end
    end

    context "when properties already carry the merge field" do
      it "does not overwrite the payload value with the input value",
         :aggregate_failures do
        inputs = [
          {
            value: "param@x.com",
            properties: {Email: "payload@x.com", Name: "P"},
          },
        ]

        expect(http).to receive(:request).with(
          :patch,
          anything,
          headers: anything,
          body: satisfy do |body|
            json = JSON.parse(body)
            json["records"][0]["fields"] == {
              "Email" => "payload@x.com",
              "Name" => "P",
            }
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: [
                {
                  "id" => "recP",
                  "fields" => {"Email" => "payload@x.com"},
                },
              ],
            }.to_json,
          }
        )

        result = adapter.batch_upsert!(
          object_type: table,
          inputs: inputs,
          match_property: "Email"
        )

        # The response carries the payload value, not the input value,
        # so the input value cannot be matched back in the mapping.
        expect(result).to eq({})
      end
    end

    it "returns a mapping keyed by the input values as provided",
       :aggregate_failures do
      inputs = [{value: "  a@b.com  ", properties: {Name: "A"}}]

      expect(http).to receive(:request).with(
        :patch,
        anything,
        headers: anything,
        body: satisfy do |body|
          json = JSON.parse(body)
          json["records"][0]["fields"] == {
            "Name" => "A",
            "Email" => "a@b.com",
          }
        end
      ).and_return(
        {
          status: 200,
          body: {
            records: [
              {
                "id" => "recA",
                "fields" => {"Email" => " a@b.com "},
              },
            ],
          }.to_json,
        }
      )

      result = adapter.batch_upsert!(
        object_type: table,
        inputs: inputs,
        match_property: "Email"
      )

      expect(result).to eq("  a@b.com  " => "recA")
    end

    context "with multiple batches (> 10 records)" do
      it "splits into slices of 10", :aggregate_failures do
        inputs = (1..12).map do |i|
          {value: "u#{i}@test.com", properties: {Name: "U#{i}"}}
        end

        # First batch: 10 records
        expect(http).to receive(:request).with(
          :patch,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            JSON.parse(body)["records"].size == 10
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: (1..10).map do |i|
                {"id" => "rec#{i}", "fields" => {"Email" => "u#{i}@test.com"}}
              end,
            }.to_json,
          }
        )

        # Second batch: 2 records
        expect(http).to receive(:request).with(
          :patch,
          "https://api.airtable.com/v0/#{base_id}/#{table}",
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: satisfy do |body|
            JSON.parse(body)["records"].size == 2
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: (11..12).map do |i|
                {"id" => "rec#{i}", "fields" => {"Email" => "u#{i}@test.com"}}
              end,
            }.to_json,
          }
        )

        result = adapter.batch_upsert!(
          object_type: table,
          inputs: inputs,
          match_property: "Email"
        )
        expect(result).to be_a(Hash)
        expect(result.size).to eq(12)
      end
    end

    context "when match_property is a field NAME" do
      it "does NOT request returnFieldsByFieldId, response keys are names" do
        inputs = [{value: "a@b.com", properties: {Name: "A"}}]

        expect(http).to receive(:request).with(
          :patch,
          anything,
          headers: anything,
          body: satisfy do |body|
            json = JSON.parse(body)
            !json.key?("returnFieldsByFieldId")
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: [
                {
                  "id" => "recA",
                  "fields" => {"Email" => "a@b.com"},
                },
              ],
            }.to_json,
          }
        )

        result = adapter.batch_upsert!(
          object_type: table,
          inputs: inputs,
          match_property: "Email"
        )

        expect(result).to eq("a@b.com" => "recA")
      end
    end

    context "when match_property is a field ID" do
      it "requests returnFieldsByFieldId so the response is keyed by field ID" do
        # Without returnFieldsByFieldId, Airtable returns fields keyed by name
        # (e.g. "🟣 Mail 1") even when the request uses field IDs. Then
        # extract_batch_mapping looks up fields["fld..."] => nil and the
        # mapping is silently empty, causing BatchSynchronizer to write
        # crm_id: nil with last_digest set, leaving the row stuck forever.
        email_field_id = "fld0aeED3e0g1qqsx"
        inputs = [{value: "a@b.com", properties: {}}]

        expect(http).to receive(:request).with(
          :patch,
          anything,
          headers: anything,
          body: satisfy do |body|
            json = JSON.parse(body)
            json["returnFieldsByFieldId"] == true &&
              json["performUpsert"]["fieldsToMergeOn"] == [email_field_id] &&
              json["records"][0]["fields"] == {email_field_id => "a@b.com"}
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: [
                {
                  "id" => "recA",
                  "fields" => {email_field_id => "a@b.com"},
                },
              ],
            }.to_json,
          }
        )

        result = adapter.batch_upsert!(
          object_type: table,
          inputs: inputs,
          match_property: email_field_id
        )

        expect(result).to eq("a@b.com" => "recA")
      end
    end

    context "with multiple batches and field ID match_property" do
      it "sends returnFieldsByFieldId on every slice", :aggregate_failures do
        email_field_id = "fld0aeED3e0g1qqsx"
        inputs = (1..12).map do |i|
          {value: "u#{i}@test.com", properties: {}}
        end

        # First batch: 10 records
        expect(http).to receive(:request).with(
          :patch,
          anything,
          headers: anything,
          body: satisfy do |body|
            json = JSON.parse(body)
            json["returnFieldsByFieldId"] == true &&
              json["records"].size == 10
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: (1..10).map do |i|
                {
                  "id" => "rec#{i}",
                  "fields" => {email_field_id => "u#{i}@test.com"},
                }
              end,
            }.to_json,
          }
        )

        # Second batch: 2 records
        expect(http).to receive(:request).with(
          :patch,
          anything,
          headers: anything,
          body: satisfy do |body|
            json = JSON.parse(body)
            json["returnFieldsByFieldId"] == true &&
              json["records"].size == 2
          end
        ).and_return(
          {
            status: 200,
            body: {
              records: (11..12).map do |i|
                {
                  "id" => "rec#{i}",
                  "fields" => {email_field_id => "u#{i}@test.com"},
                }
              end,
            }.to_json,
          }
        )

        result = adapter.batch_upsert!(
          object_type: table,
          inputs: inputs,
          match_property: email_field_id
        )

        expect(result.size).to eq(12)
        expect(result["u1@test.com"]).to eq("rec1")
        expect(result["u12@test.com"]).to eq("rec12")
      end
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.batch_upsert!(
          object_type: "",
          inputs: [{value: "a@b.com", properties: {}}],
          match_property: "Email"
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_upsert!(
          object_type: table, inputs: [], match_property: "Email"
        )
      end.to raise_error(ArgumentError, /inputs/)

      expect do
        adapter.batch_upsert!(
          object_type: table,
          inputs: [{value: "a@b.com", properties: {}}],
          match_property: ""
        )
      end.to raise_error(ArgumentError, /match_property/)
    end

    it "raises when an input carries a blank value",
       :aggregate_failures do
      expect(http).not_to receive(:request)

      expect do
        adapter.batch_upsert!(
          object_type: table,
          inputs: [{value: "   ", properties: {Name: "A"}}],
          match_property: "Email"
        )
      end.to raise_error(ArgumentError, /value/)

      expect do
        adapter.batch_upsert!(
          object_type: table,
          inputs: [{value: nil, properties: {Name: "A"}}],
          match_property: "Email"
        )
      end.to raise_error(ArgumentError, /value/)
    end

    it "raises RateLimited on 429" do
      expect(http).to receive(:request).with(
        :patch, anything, anything
      ).and_return(
        {
          status: 429,
          body: {
            error: {
              type: "RATE_LIMIT_REACHED",
              message: "Too many requests",
            },
          }.to_json,
        }
      )

      expect do
        adapter.batch_upsert!(
          object_type: table,
          inputs: [{value: "a@b.com", properties: {Name: "A"}}],
          match_property: "Email"
        )
      end.to raise_error(
        Etlify::RateLimited, /Too many requests/
      )
    end

    it "raises ValidationFailed on 422" do
      expect(http).to receive(:request).with(
        :patch, anything, anything
      ).and_return(
        {
          status: 422,
          body: {
            error: {
              type: "INVALID_REQUEST_UNKNOWN",
              message: "Invalid fields",
            },
          }.to_json,
        }
      )

      expect do
        adapter.batch_upsert!(
          object_type: table,
          inputs: [{value: "a@b.com", properties: {Name: "A"}}],
          match_property: "Email"
        )
      end.to raise_error(
        Etlify::ValidationFailed, /Invalid fields/
      )
    end
  end

  describe "#batch_update!" do
    it "PATCHes records by id (no performUpsert) and returns identity mapping",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :patch,
        "https://api.airtable.com/v0/#{base_id}/#{table}",
        headers: hash_including("Authorization" => "Bearer #{token}"),
        body: satisfy do |body|
          json = JSON.parse(body)
          !json.key?("performUpsert") &&
            json["records"].size == 2 &&
            json["records"][0]["id"] == "recA" &&
            json["records"][0]["fields"] == {"Email" => "a@b.com"} &&
            json["records"][1]["id"] == "recC"
        end
      ).and_return({status: 200, body: {records: []}.to_json})

      result = adapter.batch_update!(
        object_type: table,
        records: [
          {crm_id: "recA", properties: {Email: "a@b.com"}},
          {crm_id: "recC", properties: {Email: "c@d.com"}},
        ]
      )
      expect(result).to eq("recA" => "recA", "recC" => "recC")
    end

    it "splits into slices of 10", :aggregate_failures do
      records = (1..12).map { |i| {crm_id: "rec#{i}", properties: {N: i}} }

      expect(http).to receive(:request).with(
        :patch, "https://api.airtable.com/v0/#{base_id}/#{table}", anything
      ).twice.and_return({status: 200, body: {records: []}.to_json})

      result = adapter.batch_update!(object_type: table, records: records)
      expect(result.size).to eq(12)
    end

    it "raises NotFound when Airtable returns 404 for a dead record id" do
      allow(http).to receive(:request).and_return(
        {status: 404, body: {error: {type: "NOT_FOUND", message: "x"}}.to_json}
      )

      expect do
        adapter.batch_update!(
          object_type: table,
          records: [{crm_id: "recDead", properties: {Email: "a@b.com"}}]
        )
      end.to raise_error(Etlify::NotFound)
    end

    it "raises ArgumentError on an empty records array" do
      expect do
        adapter.batch_update!(object_type: table, records: [])
      end.to raise_error(ArgumentError, /non-empty Array/)
    end
  end

  describe "#batch_delete!" do
    context "with a single batch (<= 10 IDs)" do
      it "sends one DELETE and returns results",
         :aggregate_failures do
        crm_ids = ["recA", "recB", "recC"]

        expect(http).to receive(:request).with(
          :delete,
          satisfy do |url|
            url.include?("records%5B%5D=recA") ||
              url.include?("records[]=recA")
          end,
          headers: hash_including(
            "Authorization" => "Bearer #{token}"
          ),
          body: nil
        ).and_return(
          {
            status: 200,
            body: {
              records: [
                {"id" => "recA", "deleted" => true},
                {"id" => "recB", "deleted" => true},
                {"id" => "recC", "deleted" => true},
              ],
            }.to_json,
          }
        )

        result = adapter.batch_delete!(
          object_type: table, crm_ids: crm_ids
        )
        expect(result.size).to eq(3)
        expect(result.all? { |r| r["deleted"] == true }).to be true
      end
    end

    context "with multiple batches (> 10 IDs)" do
      it "splits into slices of 10", :aggregate_failures do
        crm_ids = (1..12).map { |i| "rec#{i}" }

        # First batch: 10 IDs
        expect(http).to receive(:request).with(
          :delete,
          satisfy { |url| url.include?("rec1") },
          anything
        ).and_return(
          {
            status: 200,
            body: {
              records: (1..10).map do |i|
                {"id" => "rec#{i}", "deleted" => true}
              end,
            }.to_json,
          }
        )

        # Second batch: 2 IDs
        expect(http).to receive(:request).with(
          :delete,
          satisfy { |url| url.include?("rec11") },
          anything
        ).and_return(
          {
            status: 200,
            body: {
              records: (11..12).map do |i|
                {"id" => "rec#{i}", "deleted" => true}
              end,
            }.to_json,
          }
        )

        result = adapter.batch_delete!(
          object_type: table, crm_ids: crm_ids
        )
        expect(result.size).to eq(12)
      end
    end

    it "raises on invalid arguments", :aggregate_failures do
      expect do
        adapter.batch_delete!(
          object_type: "", crm_ids: ["rec1"]
        )
      end.to raise_error(ArgumentError, /object_type/)

      expect do
        adapter.batch_delete!(
          object_type: table, crm_ids: []
        )
      end.to raise_error(ArgumentError, /crm_ids/)
    end

    it "raises RateLimited on 429" do
      expect(http).to receive(:request).with(
        :delete, anything, anything
      ).and_return(
        {
          status: 429,
          body: {
            error: {
              type: "RATE_LIMIT_REACHED",
              message: "Too many requests",
            },
          }.to_json,
        }
      )

      expect do
        adapter.batch_delete!(
          object_type: table, crm_ids: ["rec1"]
        )
      end.to raise_error(
        Etlify::RateLimited, /Too many requests/
      )
    end

    it "raises ApiError on 500" do
      expect(http).to receive(:request).with(
        :delete, anything, anything
      ).and_return(
        {
          status: 500,
          body: {
            error: {
              type: "SERVER_ERROR",
              message: "Server down",
            },
          }.to_json,
        }
      )

      expect do
        adapter.batch_delete!(
          object_type: table, crm_ids: ["rec1"]
        )
      end.to raise_error(Etlify::ApiError, /Server down/)
    end

    it "wraps transport errors into TransportError" do
      expect(http).to receive(:request).and_raise(
        StandardError.new("connection refused")
      )

      expect do
        adapter.batch_delete!(
          object_type: table, crm_ids: ["rec1"]
        )
      end.to raise_error(
        Etlify::TransportError, /connection refused/
      )
    end
  end

  describe "edge cases" do
    context "when crm_id is whitespace-only" do
      it "treats as absent and searches by match_property",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        expect(http).to receive(:request).with(
          :post, anything, anything
        ).and_return(
          {status: 200, body: {id: "recWS"}.to_json}
        )

        id = adapter.upsert!(
          object_type: table,
          payload: {Email: "a@b.com"},
          match_property: "Email",
          match_value: "a@b.com",
          crm_id: "   "
        )
        expect(id).to eq("recWS")
      end
    end

    context "when create returns 2xx but no id in response" do
      it "raises ApiError", :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        expect(http).to receive(:request).with(
          :post, anything, anything
        ).and_return(
          {status: 200, body: {fields: {}}.to_json}
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Name: "NoId"},
            match_property: "Name",
            match_value: "NoId"
          )
        end.to raise_error(Etlify::ApiError)
      end
    end

    context "when transport layer fails during create" do
      it "wraps into TransportError", :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {status: 200, body: {records: []}.to_json}
        )

        expect(http).to receive(:request).with(
          :post, anything, anything
        ).and_raise(StandardError.new("connection reset"))

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "a@b.com", Name: "A"},
            match_property: "Email",
            match_value: "a@b.com"
          )
        end.to raise_error(
          Etlify::TransportError, /connection reset/
        )
      end
    end

    context "when search returns 403" do
      it "raises Unauthorized" do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {
            status: 403,
            body: {
              error: {
                type: "FORBIDDEN",
                message: "Access denied",
              },
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "x@y.com"},
            match_property: "Email",
            match_value: "x@y.com"
          )
        end.to raise_error(Etlify::Unauthorized, /Access denied/)
      end
    end

    context "when delete! returns 403" do
      it "raises Unauthorized" do
        expect(http).to receive(:request).with(
          :delete, anything, anything
        ).and_return(
          {
            status: 403,
            body: {
              error: {
                type: "FORBIDDEN",
                message: "No access",
              },
            }.to_json,
          }
        )

        expect do
          adapter.delete!(
            object_type: table, crm_id: "rec1"
          )
        end.to raise_error(Etlify::Unauthorized, /No access/)
      end
    end

    context "when batch response is missing records key" do
      it "batch_upsert! returns empty hash",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :patch, anything, anything
        ).and_return(
          {status: 200, body: {}.to_json}
        )

        result = adapter.batch_upsert!(
          object_type: table,
          inputs: [{value: "a@b.com", properties: {Name: "A"}}],
          match_property: "Email"
        )
        expect(result).to eq({})
      end

      it "batch_delete! returns empty array",
         :aggregate_failures do
        expect(http).to receive(:request).with(
          :delete, anything, anything
        ).and_return(
          {status: 200, body: {}.to_json}
        )

        result = adapter.batch_delete!(
          object_type: table, crm_ids: ["rec1"]
        )
        expect(result).to eq([])
      end
    end

    context "when batch_upsert! transport fails" do
      it "wraps into TransportError" do
        expect(http).to receive(:request).and_raise(
          StandardError.new("batch timeout")
        )

        expect do
          adapter.batch_upsert!(
            object_type: table,
            inputs: [{value: "a@b.com", properties: {Name: "A"}}],
            match_property: "Email"
          )
        end.to raise_error(
          Etlify::TransportError, /batch timeout/
        )
      end
    end

    context "when update returns 409" do
      it "raises ValidationFailed", :aggregate_failures do
        expect(http).to receive(:request).with(
          :get, /filterByFormula/, anything
        ).and_return(
          {
            status: 200,
            body: {records: [{"id" => "rec9"}]}.to_json,
          }
        )

        expect(http).to receive(:request).with(
          :patch, /rec9/, anything
        ).and_return(
          {
            status: 409,
            body: {
              error: {
                type: "CONFLICT",
                message: "Record conflict",
              },
            }.to_json,
          }
        )

        expect do
          adapter.upsert!(
            object_type: table,
            payload: {Email: "x@y.com"},
            match_property: "Email",
            match_value: "x@y.com"
          )
        end.to raise_error(
          Etlify::ValidationFailed, /Record conflict/
        )
      end
    end

    context "when initialize receives non-string types" do
      it "raises ArgumentError for numeric access_token" do
        expect do
          described_class.new(
            access_token: 12345, base_id: "appXXX"
          )
        end.to raise_error(ArgumentError, /access_token/)
      end

      it "raises ArgumentError for nil base_id" do
        expect do
          described_class.new(
            access_token: "token", base_id: nil
          )
        end.to raise_error(ArgumentError, /base_id/)
      end
    end
  end

  describe "URL encoding of path segments" do
    it "URL-encodes object_type with spaces" do
      expect(http).to receive(:request).with(
        :patch, %r{My%20Contacts/recSPC}, anything
      ).and_return({status: 200, body: "{}"})

      id = adapter.upsert!(
        object_type: "My Contacts",
        payload: {Name: "Test"},
        match_property: "Name",
        match_value: "Test",
        crm_id: "recSPC"
      )
      expect(id).to eq("recSPC")
    end

    it "URL-encodes path traversal attempts" do
      expect(http).to receive(:request).with(
        :delete,
        satisfy { |url| !url.include?("/../") },
        anything
      ).and_return({status: 200, body: "{}"})

      adapter.delete!(
        object_type: table, crm_id: "../../secret"
      )
    end
  end

  describe "batch edge cases" do
    it "batch_upsert! accepts match_property as Symbol",
       :aggregate_failures do
      expect(http).to receive(:request).with(
        :patch,
        anything,
        headers: anything,
        body: satisfy do |body|
          json = JSON.parse(body)
          json["performUpsert"]["fieldsToMergeOn"] == ["Email"]
        end
      ).and_return(
        {
          status: 200,
          body: {
            records: [{"id" => "recSYM", "fields" => {"Email" => "a@b.com"}}],
          }.to_json,
        }
      )

      result = adapter.batch_upsert!(
        object_type: table,
        inputs: [{value: "a@b.com", properties: {Name: "A"}}],
        match_property: :Email
      )
      expect(result).to eq("a@b.com" => "recSYM")
    end

    it "batch raises on 2nd slice after 1st succeeds",
       :aggregate_failures do
      inputs = (1..12).map do |i|
        {value: "u#{i}@test.com", properties: {Name: "U#{i}"}}
      end

      # 1st slice succeeds
      expect(http).to receive(:request).with(
        :patch, anything,
        headers: anything,
        body: satisfy { |b| JSON.parse(b)["records"].size == 10 }
      ).and_return(
        {
          status: 200,
          body: {
            records: (1..10).map do |i|
              {"id" => "rec#{i}", "fields" => {}}
            end,
          }.to_json,
        }
      )

      # 2nd slice rate limited
      expect(http).to receive(:request).with(
        :patch, anything,
        headers: anything,
        body: satisfy { |b| JSON.parse(b)["records"].size == 2 }
      ).and_return(
        {
          status: 429,
          body: {
            error: {
              type: "RATE_LIMIT_REACHED",
              message: "Too many requests",
            },
          }.to_json,
        }
      )

      expect do
        adapter.batch_upsert!(
          object_type: table,
          inputs: inputs,
          match_property: "Email"
        )
      end.to raise_error(Etlify::RateLimited)
    end
  end
end
