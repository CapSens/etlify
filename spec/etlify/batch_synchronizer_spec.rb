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

    context "when adapter.batch_upsert! raises ValidationFailed" do
      it "falls back to per-record upsert! to isolate the offender",
         :aggregate_failures do
        user1 = create_user!(index: 1)
        user2 = create_user!(index: 2)
        user3 = create_user!(index: 3)

        # The batch upsert fails atomically (one bad record poisons it)
        allow(adapter).to receive(:batch_upsert!)
          .and_raise(Etlify::ValidationFailed.new("boom", status: 422))

        # Per-record fallback: user1 and user3 succeed, user2 fails
        allow(adapter).to receive(:upsert!) do |payload:, **|
          email = payload[:email] || payload["email"]
          case email
          when "user2@example.com"
            raise Etlify::ValidationFailed.new("bad record", status: 422)
          else
            "rec_#{email}"
          end
        end

        stats = described_class.call([user1, user2, user3], crm_name: :hubspot)

        expect(stats[:synced]).to eq(2)
        expect(stats[:errors]).to eq(1)

        line1 = CrmSynchronisation.find_by(resource: user1, crm_name: "hubspot")
        line2 = CrmSynchronisation.find_by(resource: user2, crm_name: "hubspot")
        line3 = CrmSynchronisation.find_by(resource: user3, crm_name: "hubspot")

        expect(line1.crm_id).to eq("rec_user1@example.com")
        expect(line1.error_count).to eq(0)

        expect(line2.crm_id).to be_nil
        expect(line2.error_count).to eq(1)
        expect(line2.last_error).to include("bad record")

        expect(line3.crm_id).to eq("rec_user3@example.com")
        expect(line3.error_count).to eq(0)
      end
    end

    context "when adapter.upsert! raises RateLimited during fallback" do
      it "re-raises RateLimited so BatchSyncJob can re-enqueue with backoff",
         :aggregate_failures do
        user1 = create_user!(index: 1)
        user2 = create_user!(index: 2)

        allow(adapter).to receive(:batch_upsert!)
          .and_raise(Etlify::ValidationFailed.new("boom", status: 422))

        rate_limited = Etlify::RateLimited.new("slow down", status: 429)
        allow(adapter).to receive(:upsert!).and_raise(rate_limited)

        expect do
          described_class.call([user1, user2], crm_name: :hubspot)
        end.to raise_error(Etlify::RateLimited, "slow down")

        # Neither record should have its error_count bumped: RateLimited
        # is a transient throttling signal, not a per-record failure.
        [user1, user2].each do |user|
          line = CrmSynchronisation.find_by(resource: user, crm_name: "hubspot")
          expect(line&.error_count.to_i).to eq(0)
        end
      end
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
