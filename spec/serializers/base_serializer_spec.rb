require "rails_helper"

RSpec.describe Etlify::Serializers::BaseSerializer do
  it "requires subclass to implement as_crm_payload" do
    dummy = Class.new(described_class).new(double("rec"))
    expect { dummy.as_crm_payload }.to raise_error(NotImplementedError)
  end
end
