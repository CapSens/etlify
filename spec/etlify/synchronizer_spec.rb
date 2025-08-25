# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::Synchronizer do
  let(:company) { Company.create!(name: "CapSens", domain: "capsens.eu") }
  let(:user) do
    User.create!(
      email: "dev@capsens.eu", full_name: "Emo-gilles", company_id: company.id
    )
  end

  def sync_lines_for(resource)
    CrmSynchronisation.where(resource: resource)
  end

  before do
    # Assure a stable digest strategy for deterministic tests (and overrideable).
    Etlify.configure do |c|
      c.digest_strategy = Etlify::Digest.method(:stable_sha256)
    end
  end

  context "when payload has changed (stale digest)" do
    it "upserts, updates line and returns :synced", :aggregate_failures do
      result = described_class.call(user, crm_name: :hubspot)
      expect(result).to eq(:synced)

      line = sync_lines_for(user).find_by(crm_name: "hubspot")
      expect(line).to be_present
      expect(line.crm_id).to be_present
      expect(line.last_digest).to be_present
      expect(line.last_error).to be_nil
      expect(line.last_synced_at).to be_within(2).of(Time.current)
    end
  end

  context "when payload is not modified" do
    it "only touches timestamp and returns :not_modified", :aggregate_failures do
      # First sync creates the line.
      first = described_class.call(user, crm_name: :hubspot)
      expect(first).to eq(:synced)

      travel_to(Time.current + 60) do
        # Force stale? false by setting last_digest to current digest.
        line = sync_lines_for(user).find_by(crm_name: "hubspot")
        digest = Etlify.config.digest_strategy.call(
          user.build_crm_payload(crm_name: :hubspot)
        )
        line.update!(last_digest: digest)

        res = described_class.call(user, crm_name: :hubspot)
        expect(res).to eq(:not_modified)

        line.reload
        expect(line.last_error).to be_nil
        expect(line.last_synced_at).to be_within(1).of(Time.current)
      end
    end
  end

  context "argument passing to adapter" do
    # We assert that Synchronizer passes the correct keywords to adapter.upsert!
    it "passes payload, id_property and object_type", :aggregate_failures do
      adapter_instance = instance_double(Etlify::Adapters::NullAdapter)

      # Ensure the adapter instance used is our spy
      allow(Etlify::Adapters::NullAdapter).to receive(:new)
        .and_return(adapter_instance)

      # Build the expected payload
      expected_payload = Etlify::Serializers::UserSerializer
                         .new(user).as_crm_payload

      expect(adapter_instance).to receive(:upsert!).with(
        payload: expected_payload,
        id_property: "id",
        object_type: "contacts"
      ).and_return("crm-xyz")

      result = described_class.call(user, crm_name: :hubspot)
      line = sync_lines_for(user).find_by(crm_name: "hubspot")

      expect(result).to eq(:synced)
      expect(line.crm_id).to eq("crm-xyz")
    end
  end

  context "memoization" do
    it "computes digest only once per call", :aggregate_failures do
      calls = 0
      begin
        Etlify.configure do |c|
          c.digest_strategy = lambda do |payload|
            calls += 1
            Etlify::Digest.stable_sha256(payload)
          end
        end

        # First call (stale) should invoke the strategy once even if digest
        # is used twice (stale? + update!).
        res = described_class.call(user, crm_name: :hubspot)
        expect(res).to eq(:synced)
        expect(calls).to eq(1)

        # Second call with same digest: we make stale? false by aligning last_digest
        line = sync_lines_for(user).find_by(crm_name: "hubspot")
        digest = Etlify.config.digest_strategy.call(
          user.build_crm_payload(crm_name: :hubspot)
        )
        line.update!(last_digest: digest)

        calls = 0
        res2 = described_class.call(user, crm_name: :hubspot)
        expect(res2).to eq(:not_modified)
        # stale? computes digest once
        expect(calls).to eq(1)
      ensure
        # Restore the default stable strategy (already set in before block,
        # but ensure no leak if the example fails mid-way).
        Etlify.configure do |c|
          c.digest_strategy = Etlify::Digest.method(:stable_sha256)
        end
      end
    end

    it "builds payload only once per call" do
      allow(user).to receive(:build_crm_payload).and_call_original

      res = described_class.call(user, crm_name: :hubspot)
      expect(res).to eq(:synced)
      expect(user).to have_received(:build_crm_payload).once
    end
  end

  context "when adapter raises" do
    class FailingAdapter
      def upsert!(payload:, id_property:, object_type:)
        raise "boom"
      end
    end

    it "records last_error and returns :error", :aggregate_failures do
      allow(User).to receive(:etlify_crms).and_return(
        {
          hubspot: {
            adapter: FailingAdapter,
            id_property: "id",
            crm_object_type: "contacts",
          },
        }
      )

      result = described_class.call(user, crm_name: :hubspot)
      line = sync_lines_for(user).find_by(crm_name: "hubspot")

      expect(result).to eq(:error)
      expect(line.last_error).to eq("boom")
      expect(line.last_synced_at).to be_nil
      expect(line.last_digest).to be_nil
    end
  end
end
