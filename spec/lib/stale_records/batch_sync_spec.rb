# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::StaleRecords::BatchSync do
  before do
    ActiveJob::Base.queue_adapter = :test
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
    ActiveJob::Base.queue_adapter.performed_jobs.clear
  end

  let(:logger_io) { StringIO.new }
  let(:logger)    { Logger.new(logger_io) }

  # Build a plain ids-only relation for a model.
  def ids_relation(model, ids)
    model.where(id: ids).select(model.primary_key)
  end

  # Stub Finder.call to return a mapping, honoring the `models:` filter.
  def stub_finder(mapping)
    allow(Etlify::StaleRecords::Finder)
      .to receive(:call) { |**kw|
        models = kw[:models]
        models ? mapping.slice(*models) : mapping
      }
  end

  describe ".call" do
    it "returns aggregated stats from the instance call",
       :aggregate_failures do
      allow_any_instance_of(described_class)
        .to receive(:call)
        .and_return({total: 0, per_model: {}, errors: 0})

      stats = described_class.call
      expect(stats).to eq(total: 0, per_model: {}, errors: 0)
    end
  end

  describe "#call (integration over Finder mapping)" do
    context "when dry_run is true" do
      it "counts stale ids without enqueuing or performing",
         :aggregate_failures do
        users = 3.times.map do |i|
          User.create!(email: "u#{i}@ex.com", full_name: "User #{i}")
        end
        stub_finder(User => ids_relation(User, users.map(&:id)))

        allow(Etlify::Synchronizer).to receive(:call)
        expect do
          described_class.call(
            async: true,
            dry_run: true,
            batch_size: 2,
            logger: logger
          )
        end.not_to change {
          ActiveJob::Base.queue_adapter.enqueued_jobs.size
        }

        stats = described_class.call(
          async: true,
          dry_run: true,
          batch_size: 2,
          logger: logger
        )

        expect(stats[:total]).to eq(3)
        expect(stats[:errors]).to eq(0)
        expect(stats[:per_model]).to eq("User" => 3)
        expect(Etlify::Synchronizer).not_to have_received(:call)
      end
    end

    context "when async is true" do
      it "enqueues one job per id and returns counts",
         :aggregate_failures do
        users = 5.times.map do |i|
          User.create!(email: "u#{i}@ex.com", full_name: "User #{i}")
        end
        stub_finder(User => ids_relation(User, users.map(&:id)))

        expect do
          stats = described_class.call(
            async: true,
            batch_size: 2,
            logger: logger
          )
          expect(stats[:total]).to eq(5)
          expect(stats[:errors]).to eq(0)
          expect(stats[:per_model]).to eq("User" => 5)
        end.to change {
          ActiveJob::Base.queue_adapter.enqueued_jobs.size
        }.by(5)

        job = Etlify.config.sync_job_class
        enq = ActiveJob::Base.queue_adapter.enqueued_jobs

        expect(enq.map { |j| j[:job].to_s }.uniq).to include(job.to_s)

        args = enq.map { |j| j[:args] }
        expect(args).to all(
          match([a_string_matching("User"), a_kind_of(Integer)])
        )
      end

      it "logs and re-raises when enqueue fails at batch level",
         :aggregate_failures do
        user = User.create!(email: "x@ex.com", full_name: "X")
        stub_finder(User => ids_relation(User, [user.id]))

        stub_const("DummyJob", Class.new)
        allow(DummyJob).to receive(:perform_later)
          .and_raise(StandardError, "boom")
        Etlify.config.sync_job_class = "DummyJob"

        expect do
          described_class.call(async: true, logger: logger)
        end.to raise_error(StandardError, "boom")

        expect(logger_io.string)
          .to include("[Etlify] enqueue failure for User: boom")
      ensure
        Etlify.config.sync_job_class = "Etlify::SyncJob"
      end
    end

    context "when async is false (inline)" do
      it "calls Synchronizer for each record and returns counts",
         :aggregate_failures do
        users = 4.times.map do |i|
          User.create!(email: "u#{i}@ex.com", full_name: "User #{i}")
        end
        stub_finder(User => ids_relation(User, users.map(&:id)))

        allow(Etlify::Synchronizer).to receive(:call)
        allow_any_instance_of(described_class).to receive(:sleep)

        stats = described_class.call(
          async: false,
          batch_size: 3,
          throttle: 0.01,
          logger: logger
        )

        expect(Etlify::Synchronizer).to have_received(:call).exactly(4).times
        expect(stats[:total]).to eq(4)
        expect(stats[:errors]).to eq(0)
        expect(stats[:per_model]).to eq("User" => 4)
      end

      it "counts errors without incrementing success count",
         :aggregate_failures do
        users = 3.times.map do |i|
          User.create!(email: "u#{i}@ex.com", full_name: "User #{i}")
        end
        stub_finder(User => ids_relation(User, users.map(&:id)))

        failing_id = users.first.id
        call_stub = lambda do |rec|
          raise(StandardError, "sync failed") if rec.id == failing_id
        end
        allow(Etlify::Synchronizer).to receive(:call) { |rec| call_stub.call(rec) }
        allow_any_instance_of(described_class).to receive(:sleep)

        stats = described_class.call(
          async: false,
          batch_size: 10,
          logger: logger
        )

        expect(stats[:total]).to eq(2)
        expect(stats[:errors]).to eq(1)
        expect(stats[:per_model]).to eq("User" => 2)
        expect(logger_io.string)
          .to include("[Etlify] sync failure User(id=#{failing_id}):")
      end
    end

    context "with multiple models and model filtering" do
      it "aggregates per model and honors the models filter",
         :aggregate_failures do
        c1 = Company.create!(name: "C1", domain: "c1.example")
        c2 = Company.create!(name: "C2", domain: "c2.example")
        u1 = User.create!(email: "u1@ex.com", full_name: "U1", company: c1)
        u2 = User.create!(email: "u2@ex.com", full_name: "U2", company: c2)

        full_map = {
          Company => ids_relation(Company, [c1.id, c2.id]),
          User => ids_relation(User, [u1.id, u2.id]),
        }
        stub_finder(full_map)

        expect do
          stats = described_class.call(async: true, batch_size: 2, logger: logger)
          expect(stats[:total]).to eq(4)
          expect(stats[:errors]).to eq(0)
          expect(stats[:per_model]).to eq("Company" => 2, "User" => 2)
        end.to change {
          ActiveJob::Base.queue_adapter.enqueued_jobs.size
        }.by(4)
      end
    end
  end
end
