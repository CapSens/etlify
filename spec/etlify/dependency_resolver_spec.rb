# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::DependencyResolver do
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

    # Register CRM
    reg = Etlify::CRM::RegistryItem.new(
      name: :hubspot,
      adapter: adapter,
      options: {}
    )
    allow(Etlify::CRM).to receive(:fetch).with(:hubspot).and_return(reg)
    allow(Etlify::CRM).to receive(:names).and_return([:hubspot])

    # Configure Contact
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

    # Configure Investment with crm_dependencies
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

  describe ".check" do
    context "when no crm_dependencies configured" do
      it "returns satisfied" do
        result = described_class.check(contact, crm_name: :hubspot)

        expect(result[:satisfied]).to be true
        expect(result[:missing_parents]).to be_empty
      end
    end

    context "when parent has a CrmSynchronisation with crm_id" do
      it "returns satisfied" do
        CrmSynchronisation.create!(
          crm_name: "hubspot",
          resource: contact,
          crm_id: "crm-contact-1"
        )

        result = described_class.check(investment, crm_name: :hubspot)

        expect(result[:satisfied]).to be true
        expect(result[:missing_parents]).to be_empty
      end
    end

    context "when parent has no CrmSynchronisation" do
      it "returns missing" do
        result = described_class.check(investment, crm_name: :hubspot)

        expect(result[:satisfied]).to be false
        expect(result[:missing_parents]).to eq([contact])
      end
    end

    context "when parent has CrmSynchronisation but nil crm_id" do
      it "returns missing" do
        CrmSynchronisation.create!(
          crm_name: "hubspot",
          resource: contact,
          crm_id: nil
        )

        result = described_class.check(investment, crm_name: :hubspot)

        expect(result[:satisfied]).to be false
        expect(result[:missing_parents]).to eq([contact])
      end
    end

    context "when parent association is nil" do
      it "skips and returns satisfied" do
        investment_without_contact = Investment.create!(
          contact_id: nil,
          amount: 500,
          reference: "INV-002"
        )

        # Need to allow nil contact
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

        # Must allow nil contact
        Investment.class_eval do
          belongs_to :contact, optional: true
        end

        result = described_class.check(
          investment_without_contact,
          crm_name: :hubspot
        )

        expect(result[:satisfied]).to be true
        expect(result[:missing_parents]).to be_empty
      end
    end

    context "with multiple dependencies, some satisfied and some not" do
      it "returns only the missing ones" do
        # Company is synced
        CrmSynchronisation.create!(
          crm_name: "hubspot",
          resource: company,
          crm_id: "crm-company-1"
        )
        # Contact is NOT synced

        allow(Investment).to receive(:etlify_crms).and_return(
          {
            hubspot: {
              serializer: InvestmentSerializer,
              guard: ->(_r) { true },
              crm_object_type: "deals",
              id_property: :capsens_id,
              dependencies: [
                :contact,
                :company,
              ],
              crm_dependencies: [
                :contact,
                :company,
              ],
              adapter: adapter,
              job_class: nil,
            },
          }
        )

        result = described_class.check(investment, crm_name: :hubspot)

        expect(result[:satisfied]).to be false
        expect(result[:missing_parents]).to eq([contact])
      end
    end
  end

  describe ".register_pending!" do
    it "creates SyncDependency rows" do
      described_class.register_pending!(
        investment,
        crm_name: :hubspot,
        missing_parents: [contact]
      )

      dep = Etlify::SyncDependency.last
      expect(dep.crm_name).to eq("hubspot")
      expect(dep.resource_type).to eq("Investment")
      expect(dep.resource_id).to eq(investment.id)
      expect(dep.parent_resource_type).to eq("Contact")
      expect(dep.parent_resource_id).to eq(contact.id)
    end

    it "is idempotent (does not duplicate)" do
      2.times do
        described_class.register_pending!(
          investment,
          crm_name: :hubspot,
          missing_parents: [contact]
        )
      end

      expect(Etlify::SyncDependency.count).to eq(1)
    end
  end

  describe ".resolve_dependents!" do
    before do
      Etlify::SyncDependency.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )
    end

    it "enqueues sync job for child when all deps resolved" do
      expect(Etlify::SyncJob).to receive(:perform_later).with(
        "Investment",
        investment.id,
        "hubspot"
      )

      described_class.resolve_dependents!(contact, crm_name: :hubspot)

      expect(Etlify::SyncDependency.count).to eq(0)
    end

    it "does NOT enqueue when child has remaining deps" do
      # Add another unresolved dependency
      Etlify::SyncDependency.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Company",
        parent_resource_id: company.id
      )

      expect(Etlify::SyncJob).not_to receive(:perform_later)

      described_class.resolve_dependents!(contact, crm_name: :hubspot)

      # Only the contact dependency was deleted, company remains
      expect(Etlify::SyncDependency.count).to eq(1)
      expect(
        Etlify::SyncDependency.first.parent_resource_type
      ).to eq("Company")
    end

    it "destroys resolved dependency rows" do
      described_class.resolve_dependents!(contact, crm_name: :hubspot)

      expect(Etlify::SyncDependency.count).to eq(0)
    end

    it "handles multiple children depending on same parent" do
      investment2 = Investment.create!(
        contact: contact,
        amount: 2000,
        reference: "INV-003"
      )
      Etlify::SyncDependency.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment2.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )

      expect(Etlify::SyncJob).to receive(:perform_later).with(
        "Investment", investment.id, "hubspot"
      )
      expect(Etlify::SyncJob).to receive(:perform_later).with(
        "Investment", investment2.id, "hubspot"
      )

      described_class.resolve_dependents!(contact, crm_name: :hubspot)

      expect(Etlify::SyncDependency.count).to eq(0)
    end

    it "does nothing when no pending deps exist" do
      Etlify::SyncDependency.delete_all

      expect(Etlify::SyncJob).not_to receive(:perform_later)

      described_class.resolve_dependents!(contact, crm_name: :hubspot)
    end
  end

  describe ".cleanup_for_child!" do
    it "removes all dependency rows for a child" do
      Etlify::SyncDependency.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )
      Etlify::SyncDependency.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Company",
        parent_resource_id: company.id
      )

      described_class.cleanup_for_child!(investment, crm_name: :hubspot)

      expect(Etlify::SyncDependency.count).to eq(0)
    end
  end

  describe ".table_exists?" do
    it "returns true when table exists" do
      described_class.reset_table_exists_cache!
      expect(described_class.table_exists?).to be true
    end
  end
end
