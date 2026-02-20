# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::SyncDependency do
  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }
  let(:contact) do
    Contact.create!(email: "dev@capsens.eu", full_name: "Dev", company: company)
  end
  let(:investment) do
    Investment.create!(
      contact: contact,
      company: company,
      amount: 1000,
      reference: "INV-001"
    )
  end

  describe "validations" do
    subject do
      described_class.new(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )
    end

    it { is_expected.to be_valid }

    it "requires crm_name" do
      subject.crm_name = nil
      expect(subject).not_to be_valid
    end

    it "requires resource_type" do
      subject.resource_type = nil
      expect(subject).not_to be_valid
    end

    it "requires resource_id" do
      subject.resource_id = nil
      expect(subject).not_to be_valid
    end

    it "requires parent_resource_type" do
      subject.parent_resource_type = nil
      expect(subject).not_to be_valid
    end

    it "requires parent_resource_id" do
      subject.parent_resource_id = nil
      expect(subject).not_to be_valid
    end

    it "enforces uniqueness on child+parent+crm" do
      subject.save!

      duplicate = described_class.new(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )
      expect(duplicate).not_to be_valid
    end
  end

  describe "scopes" do
    before do
      described_class.create!(
        crm_name: "hubspot",
        resource_type: "Investment",
        resource_id: investment.id,
        parent_resource_type: "Contact",
        parent_resource_id: contact.id
      )
    end

    describe ".for_crm" do
      it "filters by crm_name" do
        expect(described_class.for_crm(:hubspot).count).to eq(1)
        expect(described_class.for_crm(:salesforce).count).to eq(0)
      end
    end

    describe ".pending_for_parent" do
      it "finds deps waiting on a specific parent" do
        result = described_class.pending_for_parent(
          contact,
          crm_name: :hubspot
        )
        expect(result.count).to eq(1)
        expect(result.first.resource_type).to eq("Investment")
      end

      it "returns empty when no deps for parent" do
        result = described_class.pending_for_parent(
          company,
          crm_name: :hubspot
        )
        expect(result.count).to eq(0)
      end
    end

    describe ".pending_for_child" do
      it "finds deps for a specific child" do
        result = described_class.pending_for_child(
          investment,
          crm_name: :hubspot
        )
        expect(result.count).to eq(1)
        expect(result.first.parent_resource_type).to eq("Contact")
      end

      it "returns empty when no deps for child" do
        result = described_class.pending_for_child(
          contact,
          crm_name: :hubspot
        )
        expect(result.count).to eq(0)
      end
    end
  end
end
