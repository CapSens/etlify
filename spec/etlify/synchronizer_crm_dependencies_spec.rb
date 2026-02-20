# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::Synchronizer, "CRM dependencies" do
  let(:adapter) { Etlify::Adapters::NullAdapter.new }
  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }
  let(:contact) do
    Contact.create!(
      email: "dev@capsens.eu",
      full_name: "Dev",
      company: company
    )
  end
  let(:investment) do
    Investment.create!(
      contact: contact,
      company: company,
      amount: 1000,
      reference: "INV-001"
    )
  end

  before do
    stub_const("ContactSerializer", Class.new(Etlify::Serializers::BaseSerializer) do
      def as_crm_payload
        {capsens_id: record.id, email: record.email}
      end
    end)

    stub_const("InvestmentSerializer", Class.new(Etlify::Serializers::BaseSerializer) do
      def as_crm_payload
        contact_crm_id = record.contact
          &.crm_synchronisations
          &.find_by(crm_name: "hubspot")
          &.crm_id

        {
          capsens_id: record.id,
          reference: record.reference,
          contact: contact_crm_id,
        }
      end
    end)

    allow(Contact).to receive(:etlify_crms).and_return(
      {
        hubspot: {
          serializer: ContactSerializer,
          guard: ->(_r) { true },
          crm_object_type: "contacts",
          id_property: :capsens_id,
          dependencies: [],
          crm_dependencies: [],
          adapter: adapter,
          job_class: nil,
        },
      }
    )

    allow(Investment).to receive(:etlify_crms).and_return(
      {
        hubspot: {
          serializer: InvestmentSerializer,
          guard: ->(_r) { true },
          crm_object_type: "deals",
          id_property: :capsens_id,
          dependencies: [:contact],
          crm_dependencies: [:contact],
          adapter: adapter,
          job_class: nil,
        },
      }
    )

    Etlify.configure do |c|
      c.digest_strategy = Etlify::Digest.method(:stable_sha256)
    end
  end

  context "when parent is not synced" do
    it "returns :deferred" do
      result = described_class.call(investment, crm_name: :hubspot)

      expect(result).to eq(:deferred)
    end

    it "creates a SyncDependency record" do
      described_class.call(investment, crm_name: :hubspot)

      dep = Etlify::SyncDependency.last
      expect(dep).to be_present
      expect(dep.resource_type).to eq("Investment")
      expect(dep.resource_id).to eq(investment.id)
      expect(dep.parent_resource_type).to eq("Contact")
      expect(dep.parent_resource_id).to eq(contact.id)
      expect(dep.crm_name).to eq("hubspot")
    end

    it "does NOT call the adapter" do
      expect(adapter).not_to receive(:upsert!)

      described_class.call(investment, crm_name: :hubspot)
    end
  end

  context "when parent is synced" do
    before do
      CrmSynchronisation.create!(
        crm_name: "hubspot",
        resource: contact,
        crm_id: "crm-contact-42"
      )
    end

    it "returns :synced" do
      result = described_class.call(investment, crm_name: :hubspot)

      expect(result).to eq(:synced)
    end

    it "includes parent CRM ID in the payload" do
      expect(adapter).to receive(:upsert!).with(
        payload: hash_including(contact: "crm-contact-42"),
        id_property: :capsens_id,
        object_type: "deals"
      ).and_call_original

      described_class.call(investment, crm_name: :hubspot)
    end
  end

  context "after successful parent sync" do
    it "resolves dependents and enqueues child sync" do
      # First, defer the investment
      described_class.call(investment, crm_name: :hubspot)
      expect(Etlify::SyncDependency.count).to eq(1)

      # Now sync the contact
      expect(Etlify::SyncJob).to receive(:perform_later).with(
        "Investment",
        investment.id,
        "hubspot"
      )

      described_class.call(contact, crm_name: :hubspot)

      expect(Etlify::SyncDependency.count).to eq(0)
    end
  end

  context "chain dependencies (A depends on B depends on C)" do
    before do
      stub_const("CompanySerializer", Class.new(Etlify::Serializers::BaseSerializer) do
        def as_crm_payload
          {capsens_id: record.id, name: record.name}
        end
      end)

      allow(Company).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            serializer: CompanySerializer,
            guard: ->(_r) { true },
            crm_object_type: "companies",
            id_property: :capsens_id,
            dependencies: [],
            crm_dependencies: [],
            adapter: adapter,
            job_class: nil,
          },
        }
      )

      allow(Contact).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            serializer: ContactSerializer,
            guard: ->(_r) { true },
            crm_object_type: "contacts",
            id_property: :capsens_id,
            dependencies: [:company],
            crm_dependencies: [:company],
            adapter: adapter,
            job_class: nil,
          },
        }
      )
    end

    it "resolves the full chain when synced in order" do
      # 1. Investment deferred (contact not synced)
      expect(described_class.call(investment, crm_name: :hubspot)).to eq(:deferred)

      # 2. Contact deferred (company not synced)
      expect(described_class.call(contact, crm_name: :hubspot)).to eq(:deferred)

      # 3. Company syncs OK
      expect(Etlify::SyncJob).to receive(:perform_later).with(
        "Contact", contact.id, "hubspot"
      )
      expect(described_class.call(company, crm_name: :hubspot)).to eq(:synced)

      # 4. Contact can now sync (company has crm_id)
      expect(Etlify::SyncJob).to receive(:perform_later).with(
        "Investment", investment.id, "hubspot"
      )
      expect(described_class.call(contact, crm_name: :hubspot)).to eq(:synced)

      # 5. Investment can now sync (contact has crm_id)
      expect(described_class.call(investment, crm_name: :hubspot)).to eq(:synced)

      expect(Etlify::SyncDependency.count).to eq(0)
    end
  end

  context "not_modified also resolves dependents" do
    it "resolves dependents on :not_modified" do
      # Sync contact first
      described_class.call(contact, crm_name: :hubspot)

      # Create a dependency manually (simulating late registration)
      Etlify::SyncDependency.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )

      # Sync contact again (not_modified since digest hasn't changed)
      expect(Etlify::SyncJob).to receive(:perform_later).with(
        "Investment", investment.id, "hubspot"
      )

      line = CrmSynchronisation.find_by(
        crm_name: "hubspot",
        resource: contact
      )
      digest = Etlify.config.digest_strategy.call(
        contact.build_crm_payload(crm_name: :hubspot)
      )
      line.update!(last_digest: digest)

      result = described_class.call(contact, crm_name: :hubspot)
      expect(result).to eq(:not_modified)
      expect(Etlify::SyncDependency.count).to eq(0)
    end
  end

  context "cleanup on successful child sync" do
    it "cleans up stale dependency rows for the child" do
      # Create a stale dependency
      Etlify::SyncDependency.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )

      # Parent is synced
      CrmSynchronisation.create!(
        crm_name: "hubspot",
        resource: contact,
        crm_id: "crm-contact-42"
      )

      # Investment syncs successfully
      described_class.call(investment, crm_name: :hubspot)

      expect(Etlify::SyncDependency.count).to eq(0)
    end
  end
end
