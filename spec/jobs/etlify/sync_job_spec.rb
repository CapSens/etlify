require "rails_helper"

RSpec.describe Etlify::SyncJob do
  include AJTestAdapterHelpers

  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }

  let(:user) do
    User.create!(
      email: "dev@capsens.eu",
      full_name: "Emo-gilles",
      company_id: company.id
    )
  end

  let(:hubspot_crm) { "hubspot" }
  let(:salesforce_crm) { "salesforce" }
  let(:queue_name)    { Etlify.config.job_queue_name }
  let(:cache)         { Etlify.config.cache_store }

  before do
    # Use the test adapter without ActiveJob::TestHelper / Minitest
    aj_set_test_adapter!
    aj_clear_jobs
    # Clear cache to avoid stale enqueue locks across examples
    cache.clear if cache.respond_to?(:clear)
  end

  # Lock is per (model, id, crm_name)
  def lock_key_for(model_name, id, crm_name)
    "etlify:enqueue_lock:v2:#{model_name}:#{id}:#{crm_name}"
  end

  it "enqueues on the configured queue and dedupes per (model,id,crm)",
     :aggregate_failures do
    key = lock_key_for("User", user.id, hubspot_crm)
    cache.delete(key)

    expect do
      described_class.perform_later("User", user.id, hubspot_crm)
      # Second enqueue with the same CRM should be dropped by the lock.
      described_class.perform_later("User", user.id, hubspot_crm)
    end.to change { aj_enqueued_jobs.size }.by(1)

    job = aj_enqueued_jobs.first
    expect(job[:job]).to eq(described_class)
    expect(job[:args]).to eq(["User", user.id, hubspot_crm])
    expect(job[:queue]).to eq(queue_name)
    expect(cache.exist?(key)).to be(true)
  end

  it "does not dedupe when CRM differs (one job per CRM)",
     :aggregate_failures do
    hubspot_key = lock_key_for("User", user.id, hubspot_crm)
    salesforce_key = lock_key_for("User", user.id, salesforce_crm)
    cache.delete(hubspot_key)
    cache.delete(salesforce_key)

    described_class.perform_later("User", user.id, hubspot_crm)
    described_class.perform_later("User", user.id, salesforce_crm)

    expect(aj_enqueued_jobs.size).to eq(2)
    crm_names = aj_enqueued_jobs.map { |j| j[:args][2] }
    expect(crm_names.sort).to eq([hubspot_crm, salesforce_crm].sort)

    expect(cache.exist?(hubspot_key)).to be(true)
    expect(cache.exist?(salesforce_key)).to be(true)
  end

  it "clears the enqueue lock after perform (even on success)",
     :aggregate_failures do
    key = lock_key_for("User", user.id, hubspot_crm)
    cache.delete(key)

    described_class.perform_later("User", user.id, hubspot_crm)
    expect(cache.exist?(key)).to be(true)

    # Perform only immediate jobs (scheduled ones stay queued)
    aj_perform_enqueued_jobs

    expect(cache.exist?(key)).to be(false)
  end

  it "does nothing when the record cannot be found" do
    expect(Etlify::Synchronizer).not_to receive(:call)

    described_class.perform_later("User", -999_999, hubspot_crm)
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
      Etlify.config.cache_store.exist?(
        lock_key_for("User", user.id, "hubspot")
      )
    ).to be(false)
  end

  it "retries on StandardError and leaves a scheduled retry, while " \
     "keeping a fresh lock for that retry", :aggregate_failures do
    allow(Etlify::Synchronizer).to receive(:call).and_raise(StandardError)

    key = lock_key_for("User", user.id, hubspot_crm)
    cache.delete(key)

    described_class.perform_later("User", user.id, hubspot_crm)
    expect(cache.exist?(key)).to be(true)

    # Perform immediate job; perform fails, retry_on schedules a retry.
    # around_perform clears the lock for the initial run, then
    # around_enqueue of the retry sets it again.
    aj_perform_enqueued_jobs

    # A retry should be scheduled (with :at) and the lock should be present
    # for that scheduled retry.
    scheduled = aj_enqueued_jobs.select { |j| j[:job] == described_class }
    expect(scheduled.size).to eq(1)
    expect(scheduled.first[:args]).to eq(["User", user.id, hubspot_crm])
    expect(scheduled.first[:at]).to be_a(Numeric)

    # The lock remains because the retry was enqueued and around_enqueue ran.
    expect(cache.exist?(key)).to be(true)
  end

  it "re-enqueues after TTL expiry", :aggregate_failures do
    key = lock_key_for("User", user.id, "hubspot")
    cache.delete(key)

    described_class.perform_later("User", user.id, "hubspot")
    expect(aj_enqueued_jobs.size).to eq(1)

    # Attempt before TTL expiry -> dropped
    described_class.perform_later("User", user.id, "hubspot")
    expect(aj_enqueued_jobs.size).to eq(1)

    # After TTL -> allowed (assuming TTL <= 15 minutes)
    travel 16.minutes do
      described_class.perform_later("User", user.id, "hubspot")
    end
    expect(aj_enqueued_jobs.size).to eq(2)
  end
end
