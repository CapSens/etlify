# frozen_string_literal: true

require "rails_helper"

RSpec.describe CrmSynchronisation, type: :model do
  let(:company) do
    Company.create!(
      name: "CapSens",
      domain: "capsens.eu"
    )
  end

  let(:user) do
    User.create!(
      email: "dev@capsens.eu",
      full_name: "Emo-gilles",
      company: company
    )
  end

  describe "associations" do
    it "belongs to a polymorphic resource" do
      sync = described_class.create!(
        resource: user,
        crm_name: "hubspot",
        crm_id: "crm-1"
      )
      expect(sync.resource).to eq(user)
      expect(sync.resource_type).to eq("User")
      expect(sync.resource_id).to eq(user.id)
    end
  end

  describe "validations" do
    it "requires resource_type and resource_id" do
      sync = described_class.new(
        crm_name: "hubspot"
      )
      expect(sync).not_to be_valid
      expect(sync.errors[:resource_type]).to be_present
      expect(sync.errors[:resource_id]).to be_present
    end

    it "enforces crm_id uniqueness but allows nil" do
      # Unicité de crm_id (valeur non nulle)
      described_class.create!(
        resource: user,
        crm_name: "hubspot",
        crm_id: "dup-1"
      )

      dup_val = described_class.new(
        resource: company,
        crm_name: "hubspot",
        crm_id: "dup-1"
      )
      expect(dup_val).not_to be_valid
      expect(dup_val.errors[:crm_id]).to be_present

      # Nil est autorisé sur crm_id (avec des resources différentes)
      user2 = User.create!(
        email: "other@capsens.eu",
        full_name: "Autre",
        company: company
      )
      company2 = Company.create!(
        name: "OtherCo",
        domain: "other.tld"
      )

      a = described_class.new(
        resource: user2,
        crm_name: "hubspot",
        crm_id: nil
      )
      b = described_class.new(
        resource: company2,
        crm_name: "hubspot",
        crm_id: nil
      )
      expect(a).to be_valid
      expect(b).to be_valid
    end

    it "enforces resource_id uniqueness scoped to resource_type" do
      described_class.create!(
        resource: user,
        crm_name: "hubspot",
        crm_id: "u-1"
      )

      dup_same_resource = described_class.new(
        resource: user,
        crm_name: "hubspot",
        crm_id: "u-2"
      )
      expect(dup_same_resource).not_to be_valid
      expect(dup_same_resource.errors[:resource_id]).to be_present

      # Même id numérique mais type différent : OK
      # Pour éviter une collision de PK, on crée d'abord un user SANS company,
      # puis on crée une company avec le même id que ce user.
      lonely_user = User.create!(
        email: "lonely@capsens.eu",
        full_name: "Lonely",
        company: nil
      )

      other_company = Company.create!(
        id: lonely_user.id,
        name: "Other",
        domain: "other.tld"
      )

      ok = described_class.new(
        resource: other_company,
        crm_name: "hubspot",
        crm_id: "c-1"
      )
      expect(ok).to be_valid
    end
  end

  describe "#stale?" do
    it "returns true when digests differ, false when equal" do
      sync = described_class.create!(
        resource: user,
        crm_name: "hubspot",
        crm_id: "sync-1",
        last_digest: "OLD"
      )
      expect(sync.stale?("NEW")).to eq(true)
      expect(sync.stale?("OLD")).to eq(false)
    end
  end

  describe "scopes" do
    it ".with_error returns only rows with last_error and " \
       ".without_error the inverse" do
      ok = described_class.create!(
        resource: user,
        crm_name: "hubspot",
        crm_id: "ok-1",
        last_error: nil
      )
      bad = described_class.create!(
        resource: company,
        crm_name: "hubspot",
        crm_id: "ko-1",
        last_error: "boom"
      )
      expect(described_class.with_error).to eq([bad])
      expect(described_class.without_error).to eq([ok])
    end
  end
end
