# frozen_string_literal: true

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
    it "enqueues one BatchSyncJob per CRM with all record pairs" do
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

      # 2 users x 2 CRMs = 4 syncs to perform
      expect(stats[:total]).to eq(4)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(4)

      jobs = aj_enqueued_jobs
             .select { |j| j[:job] == Etlify::BatchSyncJob }
      expect(jobs.size).to eq(2)

      crm_names = jobs.map { |j| j[:args][0] }.sort
      expect(crm_names).to eq(["hubspot", "salesforce"])

      jobs.each do |job|
        flat_pairs = job[:args][1]
        pairs = flat_pairs.each_slice(2).to_a
        ids = pairs.map(&:last)
        expect(ids.sort).to eq([user1.id, user2.id].sort)
        pairs.each { |p| expect(p.first).to eq("User") }
      end
    end

    it "enqueues two BatchSyncJobs for one record linked to two CRMs" do
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

      expect(stats[:total]).to eq(2)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)

      jobs = aj_enqueued_jobs
             .select { |j| j[:job] == Etlify::BatchSyncJob }
      expect(jobs.size).to eq(2)

      crm_names = jobs.map { |j| j[:args][0] }.sort
      expect(crm_names).to eq(["hubspot", "salesforce"])

      jobs.each do |job|
        flat_pairs = job[:args][1]
        expect(flat_pairs).to eq(["User", user.id])
      end
    end

    it "filters by crm_name when provided" do
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
             .select { |j| j[:job] == Etlify::BatchSyncJob }
      expect(jobs.size).to eq(1)
      expect(jobs.first[:args][0]).to eq("hubspot")

      flat_pairs = jobs.first[:args][1]
      ids = flat_pairs.each_slice(2).map(&:last)
      expect(ids.sort).to eq([user1.id, user2.id].sort)
    end

    it "honors batch_size while collecting all ids per CRM" do
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

      jobs = aj_enqueued_jobs
             .select { |j| j[:job] == Etlify::BatchSyncJob }
      expect(jobs.size).to eq(1)

      flat_pairs = jobs.first[:args][1]
      ids = flat_pairs.each_slice(2).map(&:last)
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

    it "counts service errors but continues processing" do
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
        expect(Etlify::Synchronizer).to have_received(:call)
          .exactly(3).times
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
      allow(Etlify::Synchronizer).to receive(:call) do |_record, crm_name:|
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

      create_user!(index: 1)

      stats = described_class.call(async: true, batch_size: 10)

      expect(stats[:total]).to eq(1)
      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(1)
      expect(jobs.first[:job]).to eq(CustomSyncJob)
    end

    it "falls back to BatchSyncJob when no custom job_class" do
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
      expect(jobs.first[:job]).to eq(Etlify::BatchSyncJob)

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

      # 2 models x 1 record x 2 CRMs = 4
      expect(stats[:total]).to eq(4)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)
      expect(stats[:per_model]["Company"]).to eq(2)

      jobs = aj_enqueued_jobs
             .select { |j| j[:job] == Etlify::BatchSyncJob }
      # One BatchSyncJob per CRM (hubspot + salesforce)
      expect(jobs.size).to eq(2)

      crm_names = jobs.map { |j| j[:args][0] }.sort
      expect(crm_names).to eq(["hubspot", "salesforce"])
    end
  end
end
