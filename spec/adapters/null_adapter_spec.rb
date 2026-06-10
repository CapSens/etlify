require "rails_helper"

RSpec.describe Etlify::Adapters::NullAdapter do
  let(:payload) { {id: 1, any: "data"} }

  describe "#upsert!" do
    it "returns the match_value when provided" do
      expect(
        described_class.new.upsert!(
          payload: payload,
          object_type: "contacts",
          match_property: "id",
          match_value: "42"
        )
      ).to eq("42")
    end

    it "returns the crm_id when known" do
      expect(
        described_class.new.upsert!(
          payload: payload,
          object_type: "contacts",
          match_property: "id",
          match_value: "42",
          crm_id: "rec_1"
        )
      ).to eq("rec_1")
    end

    it "returns a generated id when match_value is blank" do
      expect(
        described_class.new.upsert!(
          payload: payload,
          object_type: "contacts",
          match_property: "id",
          match_value: nil
        )
      ).to be_a(String)
    end
  end

  it "delete! returns true" do
    expect(
      described_class.new.delete!(
        crm_id: "x",
        object_type: "contacts"
      )
    ).to be true
  end

  describe "#batch_upsert!" do
    it "maps each input value to a generated id" do
      mapping = described_class.new.batch_upsert!(
        object_type: "contacts",
        inputs: [
          {value: "a@example.com", properties: {any: "data"}},
          {value: "b@example.com", properties: {any: "data"}},
        ],
        match_property: "email"
      )

      expect(mapping.keys).to eq(["a@example.com", "b@example.com"])
      expect(mapping.values).to all(be_a(String))
    end
  end
end
