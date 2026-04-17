require "rails_helper"

RSpec.describe Etlify::BatchSynchronizer do
  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }
  let(:adapter) { Etlify::Adapters::NullAdapter.new }

  def create_user!(index:)
    User.create!(
      email: "user#{index}@example.com",
      full_name: "User #{index}",
      company: company
    )
  end

  before do
    allow(User).to receive(:etlify_crms).and_return(
      {
        hubspot: {
          adapter: adapter,
          id_property: "email",
          crm_object_type: "contacts",
          guard: nil,
          sync_dependencies: [],
        },
      }
    )
  end

  describe ".call" do
    it "batch upserts all records and updates sync_lines" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      stats = described_class.call([user1, user2], crm_name: :hubspot)

      expect(stats[:synced]).to eq(2)
      expect(stats[:errors]).to eq(0)

      [user1, user2].each do |user|
        line = CrmSynchronisation.find_by(
          resource: user, crm_name: "hubspot"
        )
        expect(line).to be_present
        expect(line.crm_id).to be_present
        expect(line.last_digest).to be_present
        expect(line.last_synced_at).to be_within(2).of(Time.current)
      end
    end

    it "calls adapter.batch_upsert! with all payloads" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      expect(adapter).to receive(:batch_upsert!).with(
        object_type: "contacts",
        records: [
          hash_including(email: "user1@example.com"),
          hash_including(email: "user2@example.com"),
        ],
        id_property: "email"
      ).and_call_original

      described_class.call([user1, user2], crm_name: :hubspot)
    end

    it "skips records where guard returns false" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: adapter,
            id_property: "email",
            crm_object_type: "contacts",
            guard: ->(u) { u.email != "user2@example.com" },
            sync_dependencies: [],
          },
        }
      )

      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      stats = described_class.call([user1, user2], crm_name: :hubspot)

      expect(stats[:synced]).to eq(1)
      expect(stats[:skipped]).to eq(1)
    end

    it "skips not_modified records (same digest)" do
      user = create_user!(index: 1)

      # First sync
      described_class.call([user], crm_name: :hubspot)

      # Second sync should be not_modified
      stats = described_class.call([user], crm_name: :hubspot)

      expect(stats[:synced]).to eq(0)
      expect(stats[:not_modified]).to eq(1)
    end

    it "does not call batch_upsert! when all records are skipped" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: adapter,
            id_property: "email",
            crm_object_type: "contacts",
            guard: ->(_u) { false },
            sync_dependencies: [],
          },
        }
      )

      user = create_user!(index: 1)

      expect(adapter).not_to receive(:batch_upsert!)
      stats = described_class.call([user], crm_name: :hubspot)

      expect(stats[:skipped]).to eq(1)
      expect(stats[:synced]).to eq(0)
    end

    context "when the CRM is disabled" do
      before do
        allow(Etlify::CRM).to receive(:enabled?).with(:hubspot)
                                                .and_return(false)
      end

      it "returns disabled stats and does not call batch_upsert!",
         :aggregate_failures do
        user1 = create_user!(index: 1)
        user2 = create_user!(index: 2)

        expect(adapter).not_to receive(:batch_upsert!)

        stats = described_class.call([user1, user2], crm_name: :hubspot)

        expect(stats[:disabled]).to be(true)
        expect(stats[:skipped]).to eq(2)
        expect(stats[:synced]).to eq(0)
        expect(stats[:errors]).to eq(0)
        expect(
          CrmSynchronisation.where(crm_name: "hubspot").count
        ).to eq(0)
      end
    end
  end
end
