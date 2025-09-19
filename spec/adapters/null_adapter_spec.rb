require "rails_helper"

RSpec.describe Etlify::Adapters::NullAdapter do
  let(:payload) { {id: 1, any: "data"} }

  it "returns an id for upsert!" do
    expect(
      described_class.new.upsert!(
        payload: payload,
        object_type: "contacts",
        id_property: "id"
      )
    ).to be_a(String)
  end

  it "delete! returns true" do
    expect(
      described_class.new.delete!(
        crm_id: "x",
        object_type: "contacts"
      )
    ).to be true
  end
end
