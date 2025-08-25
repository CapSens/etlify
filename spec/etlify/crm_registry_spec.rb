# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::CRM do
  before do
    # Reset registry to keep tests isolated.
    Etlify::CRM.registry.clear
  end

  it "registers CRMs and exposes names + fetch" do
    # Ensure the install hook is called on the Model DSL.
    allow(Etlify::Model).to receive(:install_dsl_for_crm)

    Etlify::CRM.register(
      :hubspot,
      adapter: Etlify::Adapters::NullAdapter,
      options: {job_class: "DummyJob"}
    )

    item = Etlify::CRM.fetch(:hubspot)
    expect(item.name).to eq(:hubspot)
    expect(item.adapter).to eq(Etlify::Adapters::NullAdapter)
    expect(item.options).to eq({job_class: "DummyJob"})
    expect(Etlify::CRM.names).to contain_exactly(:hubspot)

    expect(Etlify::Model).to have_received(:install_dsl_for_crm).with(:hubspot)
  end

  it "normalizes registry keys to symbols" do
    Etlify::CRM.register("custom", adapter: Etlify::Adapters::NullAdapter)
    expect(Etlify::CRM.fetch(:custom).name).to eq(:custom)
  end
end
