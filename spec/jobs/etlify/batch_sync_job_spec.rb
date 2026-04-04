# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::BatchSyncJob do
  include AJTestAdapterHelpers

  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }
  let(:cache) { Etlify.config.cache_store }

  before do
    aj_set_test_adapter!
    aj_clear_jobs
    cache.clear if cache.respond_to?(:clear)

    unless Etlify::CRM.registry[:hubspot]
      Etlify::CRM.register(
        :hubspot,
        adapter: Etlify::Adapters::NullAdapter.new
      )
    end
  end

  def create_user!(index:)
    User.create!(
      email: "user#{index}@example.com",
      full_name: "User #{index}",
      company: company
    )
  end

  def lock_key(crm_name)
    "etlify:batch_sync_lock:#{crm_name}"
  end

  describe "#perform with explicit record pairs" do
    it "syncs all records and creates sync_lines" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      pairs = ["User", user1.id, "User", user2.id]

      described_class.perform_now("hubspot", pairs)

      [user1, user2].each do |user|
        line = CrmSynchronisation.find_by(
          resource: user, crm_name: "hubspot"
        )
        expect(line).to be_present
        expect(line.last_digest).to be_present
      end
    end

    it "skips records that no longer exist" do
      user = create_user!(index: 1)
      pairs = ["User", user.id, "User", -999]

      described_class.perform_now("hubspot", pairs)

      expect(CrmSynchronisation.count).to eq(1)
    end

    it "uses BatchSynchronizer when adapter supports batch_upsert!" do
      user = create_user!(index: 1)
      pairs = ["User", user.id]

      expect(Etlify::BatchSynchronizer).to receive(:call).and_call_original
      described_class.perform_now("hubspot", pairs)
    end

    it "falls back to Synchronizer when adapter lacks batch_upsert!" do
      minimal_adapter = Object.new
      minimal_adapter.define_singleton_method(:upsert!) { |**_| "123" }
      minimal_adapter.define_singleton_method(:delete!) { |**_| true }

      Etlify::CRM.register(:minimal_crm, adapter: minimal_adapter)
      allow(User).to receive(:etlify_crms).and_return(
        {
          minimal_crm: {
            adapter: minimal_adapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      user = create_user!(index: 1)
      pairs = ["User", user.id]

      expect(Etlify::Synchronizer).to receive(:call).and_call_original
      expect(Etlify::BatchSynchronizer).not_to receive(:call)
      described_class.perform_now("minimal_crm", pairs)

      Etlify::CRM.registry.delete(:minimal_crm)
    end
  end

  describe "RateLimited error handling" do
    it "re-enqueues on RateLimited from batch_upsert!" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      pairs = ["User", user1.id, "User", user2.id]

      allow_any_instance_of(Etlify::Adapters::NullAdapter)
        .to receive(:batch_upsert!)
        .and_raise(Etlify::RateLimited.new("rate limited", status: 429))

      described_class.perform_now("hubspot", pairs)

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)
      expect(jobs.first[:args][0]).to eq("hubspot")
    end
  end

  describe "concurrency lock" do
    it "prevents duplicate batch jobs for the same CRM" do
      user = create_user!(index: 1)
      pairs = ["User", user.id]

      described_class.perform_later("hubspot", pairs)
      described_class.perform_later("hubspot", pairs)

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)
    end

    it "allows batch jobs for different CRMs" do
      Etlify::CRM.register(
        :salesforce,
        adapter: Etlify::Adapters::NullAdapter.new
      )

      user = create_user!(index: 1)

      described_class.perform_later("hubspot", ["User", user.id])
      described_class.perform_later("salesforce", ["User", user.id])

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(2)

      Etlify::CRM.registry.delete(:salesforce)
    end

    it "clears the lock after perform" do
      user = create_user!(index: 1)
      pairs = ["User", user.id]

      described_class.perform_later("hubspot", pairs)
      expect(cache.exist?(lock_key("hubspot"))).to be(true)

      aj_perform_enqueued_jobs

      expect(cache.exist?(lock_key("hubspot"))).to be(false)
    end
  end

  describe "discovery mode (no record_pairs)" do
    it "discovers stale records via Finder and syncs them" do
      create_user!(index: 1)
      create_user!(index: 2)

      described_class.perform_now("hubspot")

      expect(CrmSynchronisation.where(crm_name: "hubspot").count).to eq(2)
    end
  end
end
