# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::SyncJob do
  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }
  let(:user) do
    User.create!(
      email: "dev@capsens.eu",
      full_name: "Emo-gilles",
      company_id: company.id
    )
  end

  let(:crm_name)   { "hubspot" }
  let(:queue_name) { Etlify.config.job_queue_name }
  let(:cache)      { Etlify.config.cache_store }

  before do
    # Use the test adapter without ActiveJob::TestHelper / Minitest
    aj_set_test_adapter!
    aj_clear_jobs
    # Clear cache to avoid stale enqueue locks across examples
    cache.clear if cache.respond_to?(:clear)
  end

  def lock_key_for(klass_name, id)
    "etlify:jobs:sync:#{klass_name}:#{id}"
  end

  it "enqueues on the configured queue and dedupes with a cache lock",
     :aggregate_failures do
    key = lock_key_for("User", user.id)
    cache.delete(key)

    expect do
      described_class.perform_later("User", user.id, crm_name)
      # Second enqueue should be dropped by around_enqueue lock.
      described_class.perform_later("User", user.id, crm_name)
    end.to change { aj_enqueued_jobs.size }.by(1)

    job = aj_enqueued_jobs.first
    expect(job[:job]).to eq(described_class)
    expect(job[:args]).to eq(["User", user.id, crm_name])
    expect(job[:queue]).to eq(queue_name)
    expect(cache.exist?(key)).to be(true)
  end

  it "clears the enqueue lock after perform (even on success)",
     :aggregate_failures do
    key = lock_key_for("User", user.id)
    cache.delete(key)

    described_class.perform_later("User", user.id, crm_name)
    expect(cache.exist?(key)).to be(true)

    # Perform only immediate jobs (scheduled ones stay queued)
    aj_perform_enqueued_jobs

    expect(cache.exist?(key)).to be(false)
  end

  it "does nothing when the record cannot be found" do
    expect(Etlify::Synchronizer).not_to receive(:call)

    described_class.perform_later("User", -999_999, crm_name)
    aj_perform_enqueued_jobs

    expect(aj_enqueued_jobs.size).to eq(0)
  end

  it "calls Synchronizer with the record and crm_name keyword",
     :aggregate_failures do
    expect(Etlify::Synchronizer).to receive(:call).with(
      user,
      crm_name: :hubspot
    ).and_return(:synced)

    described_class.perform_later("User", user.id, "hubspot")
    aj_perform_enqueued_jobs

    expect(aj_enqueued_jobs).to be_empty
    expect(
      Etlify.config.cache_store.exist?(lock_key_for("User", user.id))
    ).to be(false)
  end

  it "retries on StandardError and leaves a scheduled retry, while " \
     "keeping a fresh lock for that retry", :aggregate_failures do
    allow(Etlify::Synchronizer).to receive(:call).and_raise(StandardError)

    key = lock_key_for("User", user.id)
    cache.delete(key)

    described_class.perform_later("User", user.id, crm_name)
    expect(cache.exist?(key)).to be(true)

    # Perform immediate job; the perform will fail, retry_on schedules a retry.
    # around_perform clears the lock for the *initial* run, then
    # around_enqueue of the retry sets it again.
    aj_perform_enqueued_jobs

    # A retry should be scheduled (with :at) and the lock should be present
    # for that scheduled retry.
    scheduled = aj_enqueued_jobs.select { |j| j[:job] == described_class }
    expect(scheduled.size).to eq(1)
    expect(scheduled.first[:args]).to eq(["User", user.id, crm_name])
    expect(scheduled.first[:at]).to be_a(Numeric)

    # The lock remains because the retry was enqueued and around_enqueue ran.
    expect(cache.exist?(key)).to be(true)
  end

  it "re-enqueues after TTL expiry", :aggregate_failures do
    key = lock_key_for("User", user.id)
    cache.delete(key)

    described_class.perform_later("User", user.id, "hubspot")
    expect(aj_enqueued_jobs.size).to eq(1)

    # Attempt before TTL expiry -> dropped
    described_class.perform_later("User", user.id, "hubspot")
    expect(aj_enqueued_jobs.size).to eq(1)

    # After TTL -> allowed
    travel 16.minutes do
      described_class.perform_later("User", user.id, "hubspot")
    end
    expect(aj_enqueued_jobs.size).to eq(2)
  end
end
