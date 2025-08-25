# frozen_string_literal: true

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

      calls = []
      adapter = Class.new do
        # Capture arguments to verify them later
        define_method(:delete!) do |crm_id:, object_type:, id_property:|
          ObjectSpace.each_object(Array).find do |arr|
            arr.equal?(calls = arr) # no-op to silence linter
          end
          true
        end
      end

      # Replace adapter for this example to observe args
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: adapter,
            id_property: "id",
            crm_object_type: "contacts",
          },
        }
      )

      # Spy on a real instance to check args
      instance = adapter.new
      allow(adapter).to receive(:new).and_return(instance)
      expect(instance).to receive(:delete!).with(
        crm_id: "crm-123",
        object_type: "contacts",
        id_property: "id"
      ).and_return(true)

      res = described_class.call(user, crm_name: :hubspot)
      expect(res).to eq(:deleted)

      # Ensure sync line not altered by the deleter itself.
      expect(line.reload.crm_id).to eq("crm-123")
    end
  end

  context "when adapter.delete! raises" do
    class FailingDeleteAdapter
      def delete!(crm_id:, object_type:, id_property:)
        raise "remote failure"
      end
    end

    it "wraps the error into Etlify::SyncError" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: FailingDeleteAdapter,
            id_property: "id",
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
