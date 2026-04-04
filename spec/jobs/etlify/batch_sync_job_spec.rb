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
    it "calls Synchronizer for each record" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      pairs = ["User", user1.id, "User", user2.id]

      calls = []
      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        calls << [record.id, crm_name]
        :synced
      end

      described_class.perform_now("hubspot", pairs)

      expect(calls).to match_array(
        [
          [user1.id, :hubspot],
          [user2.id, :hubspot],
        ]
      )
    end

    it "skips records that no longer exist" do
      user = create_user!(index: 1)
      pairs = ["User", user.id, "User", -999]

      expect(Etlify::Synchronizer).to receive(:call).once
      described_class.perform_now("hubspot", pairs)
    end

    it "continues on individual record errors" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      pairs = ["User", user1.id, "User", user2.id]

      call_count = 0
      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        call_count += 1
        raise StandardError, "boom" if record.id == user1.id

        :synced
      end

      described_class.perform_now("hubspot", pairs)

      expect(call_count).to eq(2)
    end
  end

  describe "rate limiter injection" do
    it "installs rate limiter on adapter when rate_limit is configured" do
      adapter = Etlify::Adapters::NullAdapter.new
      # Add rate_limiter accessor for test
      adapter.define_singleton_method(:rate_limiter=) { |v| @rl = v }
      adapter.define_singleton_method(:rate_limiter) { @rl }

      Etlify::CRM.register(
        :rate_limited_crm,
        adapter: adapter,
        options: {rate_limit: {max_requests: 10, period: 1}}
      )

      user = create_user!(index: 1)
      pairs = ["User", user.id]

      limiter_during_sync = nil
      allow(Etlify::Synchronizer).to receive(:call) do |_record, crm_name:|
        limiter_during_sync = adapter.rate_limiter
        :synced
      end

      described_class.perform_now("rate_limited_crm", pairs)

      expect(limiter_during_sync).to be_a(Etlify::RateLimiter)
      # Rate limiter should be removed after perform
      expect(adapter.rate_limiter).to be_nil

      Etlify::CRM.registry.delete(:rate_limited_crm)
    end

    it "does not set rate limiter when adapter does not support it" do
      adapter = Etlify::Adapters::NullAdapter.new

      Etlify::CRM.register(
        :no_rl_crm,
        adapter: adapter,
        options: {rate_limit: {max_requests: 10, period: 1}}
      )

      user = create_user!(index: 1)
      pairs = ["User", user.id]

      allow(Etlify::Synchronizer).to receive(:call).and_return(:synced)

      expect do
        described_class.perform_now("no_rl_crm", pairs)
      end.not_to raise_error

      Etlify::CRM.registry.delete(:no_rl_crm)
    end
  end

  describe "RateLimited error handling" do
    it "re-enqueues with remaining pairs on RateLimited" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      user3 = create_user!(index: 3)
      pairs = [
        "User", user1.id,
        "User", user2.id,
        "User", user3.id,
      ]

      call_count = 0
      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        call_count += 1
        if record.id == user2.id
          raise Etlify::RateLimited.new(
            "rate limited",
            status: 429
          )
        end
        :synced
      end

      described_class.perform_now("hubspot", pairs)

      # Should have processed user1, then failed on user2
      expect(call_count).to eq(2)

      # Should have re-enqueued with user2 and user3
      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)

      remaining_args = jobs.first[:args]
      expect(remaining_args[0]).to eq("hubspot")
      remaining_pairs = remaining_args[1]
      expect(remaining_pairs).to eq(
        ["User", user2.id, "User", user3.id]
      )
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

      allow(Etlify::Synchronizer).to receive(:call).and_return(:synced)

      described_class.perform_later("hubspot", pairs)
      expect(cache.exist?(lock_key("hubspot"))).to be(true)

      aj_perform_enqueued_jobs

      expect(cache.exist?(lock_key("hubspot"))).to be(false)
    end
  end

  describe "discovery mode (no record_pairs)" do
    it "discovers stale records via Finder and processes them" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      calls = []
      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        calls << record.id
        :synced
      end

      described_class.perform_now("hubspot")

      expect(calls.sort).to eq([user1.id, user2.id].sort)
    end
  end
end
