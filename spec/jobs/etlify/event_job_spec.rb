require "rails_helper"

RSpec.describe Etlify::EventJob do
  include AJTestAdapterHelpers

  let(:user)  { User.create!(email: "jane@example.com", full_name: "Jane") }
  let(:cache) { Etlify.config.cache_store }
  let(:args) do
    [
      "User",
      user.id,
      "brevo",
      "Inscription",
      {prenom: "Jane"},
      {},
      "2026-10-16T09:00:00Z",
      true,
    ]
  end

  def perform_enqueued_event_jobs
    jobs = aj_enqueued_jobs.dup
    aj_enqueued_jobs.clear
    jobs.each do |j|
      next if j[:at]

      j[:job].perform_now(*ActiveJob::Arguments.deserialize(j[:args]))
    end
  end

  before do
    aj_set_test_adapter!
    aj_clear_jobs
    cache.clear if cache.respond_to?(:clear)
  end

  describe ".lock_key" do
    it "is scoped per record and CRM, and digests every argument",
       :aggregate_failures do
      key = described_class.lock_key(*args)

      expect(key).to start_with("etlify:event_lock:User:#{user.id}:brevo:")
      expect(described_class.lock_key(*args)).to eq(key)

      args.each_index.drop(3).each do |index|
        other = args.dup
        other[index] = "changed"
        expect(described_class.lock_key(*other)).not_to eq(key)
      end
    end
  end

  it "does not dedupe two occurrences of the same event at different times" do
    later = args.dup
    later[6] = "2026-10-16T09:05:00Z"

    described_class.perform_later(*args)
    described_class.perform_later(*later)

    expect(aj_enqueued_jobs.size).to eq(2)
  end

  it "enqueues on the configured queue and dedupes identical events",
     :aggregate_failures do
    expect do
      described_class.perform_later(*args)
      described_class.perform_later(*args)
    end.to change { aj_enqueued_jobs.size }.by(1)

    job = aj_enqueued_jobs.first
    expect(job[:queue]).to eq(Etlify.config.job_queue_name)
    expect(cache.exist?(described_class.lock_key(*args))).to be(true)
  end

  it "does not dedupe events with different properties" do
    other = args.dup
    other[4] = {prenom: "Joe"}

    described_class.perform_later(*args)
    described_class.perform_later(*other)

    expect(aj_enqueued_jobs.size).to eq(2)
  end

  it "calls EventTracker with the record and clears the lock",
     :aggregate_failures do
    expect(Etlify::EventTracker).to receive(:call).with(
      user,
      crm_name: :brevo,
      name: "Inscription",
      properties: {prenom: "Jane"},
      contact_properties: {},
      occurred_at: "2026-10-16T09:00:00Z",
      sync_first: true
    ).and_return(:tracked)

    described_class.perform_later(*args)
    perform_enqueued_event_jobs

    expect(cache.exist?(described_class.lock_key(*args))).to be(false)
  end

  it "does nothing when the record cannot be found" do
    expect(Etlify::EventTracker).not_to receive(:call)

    missing = args.dup
    missing[1] = -999_999
    described_class.perform_later(*missing)
    perform_enqueued_event_jobs

    expect(aj_enqueued_jobs).to be_empty
  end

  it "schedules a retry on error and keeps a lock for it",
     :aggregate_failures do
    allow(Etlify::EventTracker).to receive(:call).and_raise(Etlify::SyncError)

    described_class.perform_later(*args)
    perform_enqueued_event_jobs

    scheduled = aj_enqueued_jobs.select { |j| j[:job] == described_class }
    expect(scheduled.size).to eq(1)
    expect(scheduled.first[:at]).to be_a(Numeric)
    expect(cache.exist?(described_class.lock_key(*args))).to be(true)
  end

  def scheduled_retries
    aj_enqueued_jobs.select { |j| j[:job] == described_class && j[:at] }
  end

  {
    "a transport error" => Etlify::TransportError.new("timeout", status: 0),
    "a 429" => Etlify::RateLimited.new("slow down", status: 429),
    "a 5xx" => Etlify::ApiError.new("server error", status: 500),
    "a buffered sync" => Etlify::SyncError.new("buffered"),
  }.each do |label, error|
    it "schedules a retry on #{label}" do
      allow(Etlify::EventTracker).to receive(:call).and_raise(error)

      described_class.perform_later(*args)
      perform_enqueued_event_jobs

      expect(scheduled_retries.size).to eq(1)
    end
  end

  {
    "a 400" => Etlify::ApiError.new("invalid", status: 400),
    "a 401" => Etlify::Unauthorized.new("unauthorized", status: 401),
    "a 404" => Etlify::NotFound.new("not found", status: 404),
    "a 422" => Etlify::ValidationFailed.new("invalid", status: 422),
    "an invalid argument" => ArgumentError.new("name must be 1 to 255 characters"),
  }.each do |label, error|
    it "discards the job without retry on #{label}", :aggregate_failures do
      allow(Etlify::EventTracker).to receive(:call).and_raise(error)

      described_class.perform_later(*args)

      expect { perform_enqueued_event_jobs }.not_to raise_error
      expect(scheduled_retries).to eq([])
      expect(cache.exist?(described_class.lock_key(*args))).to be(false)
    end
  end

  [:skipped, :sync_failed].each do |result|
    it "ends without retry when the tracker returns #{result.inspect}" do
      allow(Etlify::EventTracker).to receive(:call).and_return(result)

      described_class.perform_later(*args)
      perform_enqueued_event_jobs

      expect(scheduled_retries).to eq([])
    end
  end

  it "lets an unexpected error through without retry" do
    allow(Etlify::EventTracker).to receive(:call).and_raise(NoMethodError, "boom")

    described_class.perform_later(*args)

    expect { perform_enqueued_event_jobs }.to raise_error(NoMethodError, "boom")
  end
end
