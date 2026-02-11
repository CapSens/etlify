require "rails_helper"

# Custom job class for testing job_class option
class CustomSyncJob < ActiveJob::Base
  queue_as :custom_queue

  def perform(model_name, id, crm_name)
    # noop for testing
  end
end

RSpec.describe Etlify::StaleRecords::BatchSync do
  include AJTestAdapterHelpers

  before do
    aj_set_test_adapter!
    aj_clear_jobs
    # Clear cache between examples (keeps tests isolated)
    Etlify.config.cache_store.clear
  end

  let!(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }

  def create_user!(index:)
    User.create!(
      email: "user#{index}@example.com",
      full_name: "User #{index}",
      company: company
    )
  end

  describe ".call in async mode" do
    it "enqueues one job per stale id and per CRM when no filter is given" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      stats = described_class.call(async: true, batch_size: 10)

      # 2 users × 2 CRMs = 4 syncs to perform
      expect(stats[:total]).to eq(4)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(4)

      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(4)

      pairs = jobs.map { |job| [job[:args][0], job[:args][1]] }.uniq
      expect(pairs).to include(["User", user1.id], ["User", user2.id])

      crm_names = jobs.map { |job| job[:args][2] }.uniq
      expect(crm_names).to match_array(["hubspot", "salesforce"])
    end

    it "enqueues two jobs for one record linked to two CRMs" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      user = create_user!(index: 1)

      stats = described_class.call(async: true, batch_size: 10)

      # Same record, two CRMs => 2 syncs to perform
      expect(stats[:total]).to eq(2)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)

      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(2)

      job_user_ids = jobs.map { |job| job[:args][1] }
      expect(job_user_ids).to all(eq(user.id))

      crm_names = jobs.map { |job| job[:args][2] }.uniq
      expect(crm_names).to match_array(["hubspot", "salesforce"])
    end

    it "filters by crm_name when provided (only that CRM is enqueued)" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      stats = described_class.call(
        async: true,
        batch_size: 10,
        crm_name: :hubspot
      )

      expect(stats[:total]).to eq(2)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)

      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(2)

      jobs.each do |job|
        model_name, user_id, crm_name = job[:args]
        expect(model_name).to eq("User")
        expect([user1.id, user2.id]).to include(user_id)
        expect(crm_name).to eq("hubspot")
      end
    end

    it "honors batch_size while enqueueing all ids per CRM" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)
      user3 = create_user!(index: 3)

      stats = described_class.call(async: true, batch_size: 2)

      expect(stats[:total]).to eq(3)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(3)

      ids = aj_enqueued_jobs.map { |job| job[:args][1] }
      expect(ids.sort).to eq([user1.id, user2.id, user3.id].sort)
    end

    it "returns zeros when there is nothing to sync" do
      allow(User).to receive(:etlify_crms).and_return({})

      stats = described_class.call(async: true, batch_size: 10)

      expect(stats[:total]).to eq(0)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]).to eq({})
      expect(aj_enqueued_jobs).to be_empty
    end
  end

  describe ".call in sync mode (inline)" do
    before do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )
    end

    it "invokes Synchronizer with the proper crm_name for each record" do
      user1 = create_user!(index: 1)
      user2 = create_user!(index: 2)

      synchronizer_calls = []
      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        synchronizer_calls << [record.class.name, record.id, crm_name]
      end

      stats = described_class.call(async: false, batch_size: 10)

      expect(stats[:total]).to eq(2)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)

      expect(Etlify::Synchronizer).to have_received(:call).twice
      expect(synchronizer_calls).to match_array(
        [
          ["User", user1.id, :hubspot],
          ["User", user2.id, :hubspot],
        ]
      )
    end

    it "counts service errors but continues processing other records" do
      create_user!(index: 1)
      user2 = create_user!(index: 2)
      create_user!(index: 3)

      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        (record.id == user2.id) ? :error : :synced
      end

      stats = described_class.call(async: false, batch_size: 10)

      aggregate_failures do
        expect(stats[:total]).to eq(3)
        expect(stats[:errors]).to eq(1)
        expect(stats[:per_model]["User"]).to eq(3)
        expect(Etlify::Synchronizer).to have_received(:call).exactly(3).times
      end
    end

    it "restricts to the provided crm_name when passed" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      create_user!(index: 1)
      create_user!(index: 2)

      called_crms = []
      allow(Etlify::Synchronizer).to receive(:call) do |record, crm_name:|
        called_crms << crm_name
        true
      end

      stats = described_class.call(
        async: false,
        batch_size: 10,
        crm_name: :hubspot
      )

      expect(stats[:total]).to eq(2)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)
      expect(called_crms).to all(eq(:hubspot))
    end
  end

  describe "custom job_class option" do
    before do
      Etlify::CRM.register(
        :custom_crm,
        adapter: Etlify::Adapters::NullAdapter.new,
        options: {job_class: "CustomSyncJob"}
      )
    end

    after do
      Etlify::CRM.registry.delete(:custom_crm)
    end

    it "uses the custom job_class when defined in CRM options" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          custom_crm: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      user = create_user!(index: 1)

      stats = described_class.call(async: true, batch_size: 10)

      expect(stats[:total]).to eq(1)
      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(1)
      expect(jobs.first[:job]).to eq(CustomSyncJob)
      expect(jobs.first[:args]).to eq(["User", user.id, "custom_crm"])
    end

    it "falls back to Etlify::SyncJob when no custom job_class is defined" do
      Etlify::CRM.register(
        :default_job_crm,
        adapter: Etlify::Adapters::NullAdapter.new,
        options: {}
      )

      allow(User).to receive(:etlify_crms).and_return(
        {
          default_job_crm: {
            adapter: Etlify::Adapters::NullAdapter.new,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      create_user!(index: 1)

      stats = described_class.call(async: true, batch_size: 10)

      expect(stats[:total]).to eq(1)
      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(1)
      expect(jobs.first[:job]).to eq(Etlify::SyncJob)

      Etlify::CRM.registry.delete(:default_job_crm)
    end
  end

  describe "multiple models in async mode" do
    it "aggregates per_model counts across models and CRMs" do
      crm_config = {
        hubspot: {
          adapter: Etlify::Adapters::NullAdapter.new,
          id_property: "email",
          crm_object_type: "contacts",
        },
        salesforce: {
          adapter: Etlify::Adapters::NullAdapter.new,
          id_property: "email",
          crm_object_type: "contacts",
        },
      }

      allow(User).to receive(:etlify_crms).and_return(crm_config)
      allow(Company).to receive(:etlify_crms).and_return(crm_config)

      create_user!(index: 1) # company is already created by let!

      stats = described_class.call(async: true, batch_size: 10)

      # 2 models × 1 record × 2 CRMs = 4
      expect(stats[:total]).to eq(4)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)
      expect(stats[:per_model]["Company"]).to eq(2)

      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(4)

      jobs_by_model = jobs.map { |job| job[:args][0] }.tally
      expect(jobs_by_model).to eq("User" => 2, "Company" => 2)

      crm_names = jobs.map { |job| job[:args][2] }.uniq
      expect(crm_names).to match_array(["hubspot", "salesforce"])
    end
  end
end
