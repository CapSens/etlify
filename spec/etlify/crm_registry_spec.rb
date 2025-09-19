require "rails_helper"

RSpec.describe Etlify::CRM do
  # Minimal adapter instance used for registration
  let(:adapter_instance) do
    Class.new do
      def upsert!(**)
      end
    end.new
  end

  before do
    # Reset registry between examples to avoid cross-test pollution.
    # We rely on the public API to clear state.
    described_class.registry.clear
  end

  it "registers CRMs and exposes names + fetch" do
    described_class.register(
      :hubspot,
      adapter: adapter_instance,
      options: {job_class: "X"}
    )

    item = described_class.fetch(:hubspot)

    expect(item.name).to eq(:hubspot)
    expect(item.adapter).to eq(adapter_instance)
    expect(item.options[:job_class]).to eq("X")
    expect(described_class.names).to include(:hubspot)
  end

  it "normalizes registry keys to symbols" do
    described_class.register("salesforce", adapter: adapter_instance)

    expect(described_class.names).to include(:salesforce)
    expect(described_class.fetch(:salesforce).name).to eq(:salesforce)
  end

  it "raises when adapter is a class (must be an instance)" do
    klass = Class.new do
  def upsert!(**)
  end
end

    expect do
      described_class.register(:bad, adapter: klass)
    end.to raise_error(
      ArgumentError,
      "Adapter must be an instance, not a class"
    )
  end

  it "does not mutate given options and stores a copy" do
    opts = {job_class: "X"}
    described_class.register(
      :pipedrive,
      adapter: adapter_instance,
      options: opts
    )

    opts[:job_class] = "Y" # mutate original hash

    item = described_class.fetch(:pipedrive)
    expect(item.options[:job_class]).to eq("X")
  end

  it "supports multiple registrations and keeps them independent" do
    a1 = Class.new do
  def upsert!(**)
  end
end.new
    a2 = Class.new do
  def upsert!(**)
  end
end.new

    described_class.register(:hubspot, adapter: a1, options: {job: "A"})
    described_class.register(:zoho, adapter: a2, options: {job: "B"})

    hubspot = described_class.fetch(:hubspot)
    zoho = described_class.fetch(:zoho)

    expect(hubspot.adapter).to eq(a1)
    expect(zoho.adapter).to eq(a2)
    expect(hubspot.options[:job]).to eq("A")
    expect(zoho.options[:job]).to eq("B")
    expect(described_class.names).to match_array(%i[hubspot zoho])
  end

  it "fetch raises when CRM is not registered" do
    expect { described_class.fetch(:unknown) }.to raise_error(KeyError)
  end
end
