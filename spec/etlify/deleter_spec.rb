require "rails_helper"

RSpec.describe Etlify::Deleter do
  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }
  let(:user) do
    User.create!(
      email: "dev@capsens.eu", full_name: "Emo-gilles", company_id: company.id
    )
  end

  def create_line(resource, crm_name:, crm_id:)
    CrmSynchronisation.create!(
      resource: resource, crm_name: crm_name, crm_id: crm_id
    )
  end

  context "when no sync line exists" do
    it "returns :noop and does not call adapter.delete!" do
      adapter = instance_double(Etlify::Adapters::NullAdapter)
      allow(Etlify::Adapters::NullAdapter).to receive(:new).and_return(adapter)
      expect(adapter).not_to receive(:delete!)

      res = described_class.call(user, crm_name: :hubspot)
      expect(res).to eq(:noop)
    end
  end

  context "when sync line exists without crm_id" do
    it "returns :noop and does not call adapter.delete!" do
      create_line(user, crm_name: "hubspot", crm_id: nil)

      adapter = instance_double(Etlify::Adapters::NullAdapter)
      allow(Etlify::Adapters::NullAdapter).to receive(:new).and_return(adapter)
      expect(adapter).not_to receive(:delete!)

      res = described_class.call(user, crm_name: :hubspot)
      expect(res).to eq(:noop)
    end
  end

  context "when sync line exists with crm_id" do
    it "calls adapter.delete! with params and returns :deleted" do
      line = create_line(user, crm_name: "hubspot", crm_id: "crm-123")

      adapter_class = Class.new do
        # Keep English comments and ≤85 chars per line
        define_method(:delete!) do |crm_id:, object_type:|
          true
        end
      end
      adapter_instance = adapter_class.new

      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: adapter_instance,
            id_property: "id",
            crm_object_type: "contacts",
          },
        }
      )

      expect(adapter_instance).to receive(:delete!).with(
        crm_id: "crm-123",
        object_type: "contacts"
      ).and_return(true)

      res = described_class.call(user, crm_name: :hubspot)
      expect(res).to eq(:deleted)
      expect(line.reload.crm_id).to eq("crm-123")
    end
  end

  context "when the CRM is disabled" do
    around do |example|
      previous = Etlify::CRM.registry[:hubspot]
      Etlify::CRM.register(
        :hubspot,
        adapter: Etlify::Adapters::NullAdapter.new,
        enabled: false
      )
      example.run
    ensure
      if previous
        Etlify::CRM.registry[:hubspot] = previous
      else
        Etlify::CRM.registry.delete(:hubspot)
      end
    end

    it "returns :disabled and does not call adapter.delete!" do
      create_line(user, crm_name: "hubspot", crm_id: "crm-123")

      adapter = instance_double(Etlify::Adapters::NullAdapter)
      allow(Etlify::Adapters::NullAdapter).to receive(:new).and_return(adapter)
      expect(adapter).not_to receive(:delete!)

      res = described_class.call(user, crm_name: :hubspot)
      expect(res).to eq(:disabled)
    end
  end

  context "when adapter.delete! raises" do
    class FailingDeleteAdapter # rubocop:disable Lint/ConstantDefinitionInBlock
      def delete!(crm_id:, object_type:)
        raise "remote failure"
      end
    end

    it "wraps the error into Etlify::SyncError" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: FailingDeleteAdapter.new,
            crm_object_type: "contacts",
          },
        }
      )

      create_line(user, crm_name: "hubspot", crm_id: "crm-err")

      expect do
        described_class.call(user, crm_name: :hubspot)
      end.to raise_error(Etlify::SyncError, /remote failure/)
    end
  end
end
