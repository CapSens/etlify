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

  def chunk_lock_key(crm_name, pairs)
    normalized = pairs.each_slice(2)
                      .map { |model, id| [model.to_s, id.to_s] }
                      .sort
    digest = ::Digest::SHA256.hexdigest(JSON.generate(normalized))
    "etlify:batch_sync_lock:#{crm_name}:chunk:#{digest}"
  end

  def discovery_lock_key(crm_name)
    "etlify:batch_sync_lock:#{crm_name}:discovery"
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
            match_by: {property: :email, value: :email},
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

    it "keeps the re-enqueued job's lock alive when pairs are identical",
       :aggregate_failures do
      user1 = create_user!(index: 1)
      pairs = ["User", user1.id]

      allow_any_instance_of(Etlify::Adapters::NullAdapter)
        .to receive(:batch_upsert!)
        .and_raise(Etlify::RateLimited.new("rate limited", status: 429))

      described_class.perform_later("hubspot", pairs)
      expect(cache.exist?(chunk_lock_key("hubspot", pairs))).to be(true)

      # Performs the original job only: the retry is scheduled with a
      # wait (:at) and stays in the queue.
      aj_perform_enqueued_jobs

      retry_jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(retry_jobs.size).to eq(1)
      expect(retry_jobs.first[:at]).to be_present
      expect(retry_jobs.first[:args][1]).to eq(pairs)

      # The retried job holds the lock for its own (identical) pairs: an
      # identical enqueue during the wait window must still be deduplicated.
      expect(cache.exist?(chunk_lock_key("hubspot", pairs))).to be(true)
    end

    it "switches from the discovery lock to a chunk lock when rate-limited",
       :aggregate_failures do
      user = create_user!(index: 1)

      allow_any_instance_of(Etlify::Adapters::NullAdapter)
        .to receive(:batch_upsert!)
        .and_raise(Etlify::RateLimited.new("rate limited", status: 429))

      described_class.perform_later("hubspot")
      expect(cache.exist?(discovery_lock_key("hubspot"))).to be(true)

      aj_perform_enqueued_jobs

      # The discovery lock is released: a new discovery run can enqueue
      # during the retry's wait window.
      expect(cache.exist?(discovery_lock_key("hubspot"))).to be(false)

      # The retry carries the discovered pairs (chunk mode) and holds its
      # own chunk lock.
      retry_jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(retry_jobs.size).to eq(1)
      expect(retry_jobs.first[:at]).to be_present
      expect(retry_jobs.first[:args][1]).to eq(["User", user.id])
      expect(
        cache.exist?(chunk_lock_key("hubspot", ["User", user.id]))
      ).to be(true)
    end

    it "keeps the retry lock alive in sequential mode too",
       :aggregate_failures do
      minimal_adapter = Object.new
      minimal_adapter.define_singleton_method(:upsert!) { |**_| "123" }
      minimal_adapter.define_singleton_method(:delete!) { |**_| true }
      Etlify::CRM.register(:seq_crm, adapter: minimal_adapter)

      user = create_user!(index: 1)
      pairs = ["User", user.id]

      allow(Etlify::Synchronizer).to receive(:call)
        .and_raise(Etlify::RateLimited.new("rate limited", status: 429))

      described_class.perform_later("seq_crm", pairs)
      expect(cache.exist?(chunk_lock_key("seq_crm", pairs))).to be(true)

      aj_perform_enqueued_jobs

      retry_jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(retry_jobs.size).to eq(1)
      expect(cache.exist?(chunk_lock_key("seq_crm", pairs))).to be(true)

      Etlify::CRM.registry.delete(:seq_crm)
    end

    it "re-enqueues only unprocessed groups after a mid-batch rate limit",
       :aggregate_failures do
      user = create_user!(index: 1)
      pairs = ["User", user.id, "Company", company.id]

      allow(Etlify::BatchSynchronizer).to receive(:call) do |records, **|
        if records.first.is_a?(Company)
          raise Etlify::RateLimited.new("rate limited", status: 429)
        end

        {synced: records.size, errors: 0}
      end

      described_class.perform_now("hubspot", pairs)

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)

      # The User group was processed: only the Company pairs are retried.
      expect(jobs.first[:args][1]).to eq(["Company", company.id])

      # The retry is delayed by DEFAULT_RETRY_AFTER seconds.
      expect(jobs.first[:at].to_f).to be_within(5).of(
        Time.current.to_f + described_class::DEFAULT_RETRY_AFTER
      )
    end

    it "re-enqueues remaining pairs when batch fails mid-way" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      user3 = create_user!(index: 3)

      batch_call_count = 0
      allow(Etlify::BatchSynchronizer).to receive(:call) do |records, **kwargs|
        batch_call_count += 1
        raise Etlify::RateLimited.new("rate limited", status: 429)
      end

      pairs = ["User", user1.id, "User", user2.id, "User", user3.id]
      described_class.perform_now("hubspot", pairs)

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)

      re_enqueued_pairs = jobs.first[:args][1].each_slice(2).to_a
      re_enqueued_ids = re_enqueued_pairs.map(&:last)
      expect(re_enqueued_ids).to include(user1.id, user2.id, user3.id)
    end

    it "re-enqueues on RateLimited in sequential mode" do
      minimal_adapter = Object.new
      minimal_adapter.define_singleton_method(:upsert!) { |**_| "123" }
      minimal_adapter.define_singleton_method(:delete!) { |**_| true }

      Etlify::CRM.register(:seq_crm, adapter: minimal_adapter)
      allow(User).to receive(:etlify_crms).and_return(
        {
          seq_crm: {
            adapter: minimal_adapter,
            match_by: {property: :email, value: :email},
            crm_object_type: "contacts",
          },
        }
      )

      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      pairs = ["User", user1.id, "User", user2.id]

      allow(Etlify::Synchronizer).to receive(:call)
        .and_raise(Etlify::RateLimited.new("rate limited", status: 429))

      described_class.perform_now("seq_crm", pairs)

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)
      expect(jobs.first[:args][0]).to eq("seq_crm")

      re_enqueued_pairs = jobs.first[:args][1].each_slice(2).to_a
      expect(re_enqueued_pairs.size).to eq(2)

      Etlify::CRM.registry.delete(:seq_crm)
    end
  end

  describe "sequential mode error handling" do
    it "skips records that raise and continues with the rest" do
      minimal_adapter = Object.new
      minimal_adapter.define_singleton_method(:upsert!) { |**_| "123" }
      minimal_adapter.define_singleton_method(:delete!) { |**_| true }

      Etlify::CRM.register(:error_crm, adapter: minimal_adapter)
      allow(User).to receive(:etlify_crms).and_return(
        {
          error_crm: {
            adapter: minimal_adapter,
            match_by: {property: :email, value: :email},
            crm_object_type: "contacts",
          },
        }
      )

      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      pairs = ["User", user1.id, "User", user2.id]

      call_count = 0
      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        call_count += 1
        raise StandardError, "boom" if call_count == 1

        :synced
      end

      described_class.perform_now("error_crm", pairs)

      # Both records were attempted (no early abort)
      expect(Etlify::Synchronizer).to have_received(:call).twice

      Etlify::CRM.registry.delete(:error_crm)
    end
  end

  describe "concurrency lock" do
    it "prevents duplicate chunk jobs with identical pairs for the same CRM" do
      user = create_user!(index: 1)
      pairs = ["User", user.id]

      described_class.perform_later("hubspot", pairs)
      described_class.perform_later("hubspot", pairs)

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)
    end

    it "deduplicates chunks carrying the same pairs in a different order" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      described_class.perform_later(
        "hubspot", ["User", user1.id, "User", user2.id]
      )
      described_class.perform_later(
        "hubspot", ["User", user2.id, "User", user1.id]
      )

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)
    end

    it "deduplicates chunks whose ids differ only by type" do
      user = create_user!(index: 1)

      described_class.perform_later("hubspot", ["User", user.id])
      described_class.perform_later("hubspot", ["User", user.id.to_s])

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)
    end

    it "allows different chunks for the same CRM to be enqueued in parallel" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      described_class.perform_later("hubspot", ["User", user1.id])
      described_class.perform_later("hubspot", ["User", user2.id])

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(2)
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

    it "prevents concurrent discovery runs for the same CRM" do
      described_class.perform_later("hubspot")
      described_class.perform_later("hubspot")

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(1)
    end

    it "does not let a chunk job collide with a discovery run for the same CRM" do
      user = create_user!(index: 1)

      described_class.perform_later("hubspot")
      described_class.perform_later("hubspot", ["User", user.id])

      jobs = aj_enqueued_jobs.select { |j| j[:job] == described_class }
      expect(jobs.size).to eq(2)
    end

    it "clears the chunk lock after perform" do
      user = create_user!(index: 1)
      pairs = ["User", user.id]

      described_class.perform_later("hubspot", pairs)
      expect(cache.exist?(chunk_lock_key("hubspot", pairs))).to be(true)

      aj_perform_enqueued_jobs

      expect(cache.exist?(chunk_lock_key("hubspot", pairs))).to be(false)
    end

    it "clears the discovery lock after perform" do
      described_class.perform_later("hubspot")
      expect(cache.exist?(discovery_lock_key("hubspot"))).to be(true)

      aj_perform_enqueued_jobs

      expect(cache.exist?(discovery_lock_key("hubspot"))).to be(false)
    end

    it "clears the discovery lock even when perform raises" do
      create_user!(index: 1)
      allow(Etlify::BatchSynchronizer).to receive(:call)
        .and_raise(RuntimeError, "unexpected failure")

      described_class.perform_later("hubspot")
      expect(cache.exist?(discovery_lock_key("hubspot"))).to be(true)

      expect { aj_perform_enqueued_jobs }.to raise_error(RuntimeError)

      expect(cache.exist?(discovery_lock_key("hubspot"))).to be(false)
    end

    it "clears the chunk lock even when perform raises" do
      user = create_user!(index: 1)
      pairs = ["User", user.id]

      allow(Etlify::BatchSynchronizer).to receive(:call)
        .and_raise(RuntimeError, "unexpected failure")

      described_class.perform_later("hubspot", pairs)
      expect(cache.exist?(chunk_lock_key("hubspot", pairs))).to be(true)

      expect { aj_perform_enqueued_jobs }.to raise_error(RuntimeError)

      expect(cache.exist?(chunk_lock_key("hubspot", pairs))).to be(false)
    end
  end

  describe "discovery mode (no record_pairs)" do
    it "discovers stale records via Finder and syncs them" do
      create_user!(index: 1)
      create_user!(index: 2)

      described_class.perform_now("hubspot")

      expect(CrmSynchronisation.where(crm_name: "hubspot").count).to eq(2)
    end

    it "only discovers stale records, not already-synced ones" do
      stale_user = create_user!(index: 1)
      synced_user = create_user!(index: 2)

      # Sync user2 so it becomes non-stale
      Etlify::Synchronizer.call(synced_user, crm_name: :hubspot)

      # Spy on BatchSynchronizer to capture which records are passed
      synced_records = []
      allow(Etlify::BatchSynchronizer).to receive(:call)
        .and_wrap_original do |method, records, **kwargs|
          synced_records.concat(records)
          method.call(records, **kwargs)
        end

      described_class.perform_now("hubspot")

      # Only the stale user should have been discovered and passed
      # to the synchronizer
      expect(synced_records.map(&:id)).to eq([stale_user.id])
    end
  end
end
