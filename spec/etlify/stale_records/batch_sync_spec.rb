# spec/etlify/stale_records/batch_sync_spec.rb
# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::StaleRecords::BatchSync do
  include AJTestAdapterHelpers

  before do
    aj_set_test_adapter!
    aj_clear_jobs
    # Clear enqueue locks between examples to avoid cross-test interference
    Etlify.config.cache_store.clear
  end

  let!(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }

  def create_user!(idx:)
    User.create!(
      email: "user#{idx}@example.com",
      full_name: "User #{idx}",
      company: company
    )
  end

  describe ".call in async mode" do
    it "enqueues one job per stale id for all CRMs when no filter is given" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      u1 = create_user!(idx: 1)
      u2 = create_user!(idx: 2)

      stats = described_class.call(async: true, batch_size: 10)

      # Stats count per CRM (2 users × 2 CRMs)
      expect(stats[:total]).to eq(4)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(4)

      # Enqueue lock collapses multi-CRM into one job per record
      jobs = aj_enqueued_jobs
      expect(jobs.size).to eq(2)

      jobs.each do |j|
        model, id, crm = j[:args]
        expect(model).to eq("User")
        expect([u1.id, u2.id]).to include(id)
        # First CRM wins because enqueue_lock ignores crm_name
        expect(crm).to eq("hubspot")
      end
    end

    it "filters by crm_name when provided (only that CRM is enqueued)" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      u1 = create_user!(idx: 1)
      u2 = create_user!(idx: 2)

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
      jobs.each do |j|
        model, id, crm = j[:args]
        expect(model).to eq("User")
        expect([u1.id, u2.id]).to include(id)
        expect(crm).to eq("hubspot")
      end
    end

    it "honors batch_size while enqueueing all ids once" do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      u1 = create_user!(idx: 1)
      u2 = create_user!(idx: 2)
      u3 = create_user!(idx: 3)

      stats = described_class.call(async: true, batch_size: 2)

      expect(stats[:total]).to eq(3)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(3)

      ids = aj_enqueued_jobs.map { |j| j[:args][1] }
      expect(ids.sort).to eq([u1.id, u2.id, u3.id].sort)
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
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )
    end

    it "invokes Synchronizer with the proper crm_name for each record" do
      u1 = create_user!(idx: 1)
      u2 = create_user!(idx: 2)

      calls = []
      allow(Etlify::Synchronizer).to receive(:call) do |rec, crm_name:|
        calls << [rec.class.name, rec.id, crm_name]
      end

      stats = described_class.call(async: false, batch_size: 10)

      expect(stats[:total]).to eq(2)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)

      expect(Etlify::Synchronizer).to have_received(:call).twice
      expect(calls).to match_array(
        [
          ["User", u1.id, :hubspot],
          ["User", u2.id, :hubspot],
        ]
      )
    end

    it "counts errors but continues processing other records" do
      create_user!(idx: 1)
      u2 = create_user!(idx: 2)
      create_user!(idx: 3)

      allow(Etlify::Synchronizer).to receive(:call) do |rec, crm_name:|
        raise "boom" if rec.id == u2.id

        true
      end

      stats = described_class.call(async: false, batch_size: 10)

      expect(stats[:total]).to eq(2)  # 2 successes
      expect(stats[:errors]).to eq(1)
      expect(stats[:per_model]["User"]).to eq(2)

      expect(Etlify::Synchronizer).to have_received(:call).exactly(3).times
    end

    it "restricts to the provided crm_name when passed" do
      # Pretend model has two CRMs; we filter on :hubspot in the call.
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
          salesforce: {
            adapter: Etlify::Adapters::NullAdapter,
            id_property: "email",
            crm_object_type: "contacts",
          },
        }
      )

      create_user!(idx: 1)
      create_user!(idx: 2)

      calls = []
      allow(Etlify::Synchronizer).to receive(:call) do |rec, crm_name:|
        calls << crm_name
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
      expect(calls).to all(eq(:hubspot))
    end
  end

  describe "multiple models in async mode" do
    it "aggregates per_model counts across models" do
      crm_conf = {
        hubspot: {
          adapter: Etlify::Adapters::NullAdapter,
          id_property: "email",
          crm_object_type: "contacts",
        },
        salesforce: {
          adapter: Etlify::Adapters::NullAdapter,
          id_property: "email",
          crm_object_type: "contacts",
        },
      }
      allow(User).to receive(:etlify_crms).and_return(crm_conf)
      allow(Company).to receive(:etlify_crms).and_return(crm_conf)

      create_user!(idx: 1)
      # company already created by let!

      stats = described_class.call(async: true, batch_size: 10)

      # Stats are per CRM: 2 models × 1 record each × 2 CRMs = 4
      expect(stats[:total]).to eq(4)
      expect(stats[:errors]).to eq(0)
      expect(stats[:per_model]["User"]).to eq(2)
      expect(stats[:per_model]["Company"]).to eq(2)

      # Enqueue lock => one job per record (2 jobs total), crm = "hubspot"
      expect(aj_enqueued_jobs.size).to eq(2)
      aj_enqueued_jobs.each do |j|
        model, _id, crm = j[:args]
        expect(%w[User Company]).to include(model)
        expect(crm).to eq("hubspot")
      end
    end
  end
end
