require "rails_helper"

RSpec.describe Etlify::EventTracker do
  let(:user) { User.create!(email: "jane@example.com", full_name: "Jane") }

  let(:adapter_class) do
    Class.new do
      attr_reader :events

      def initialize(crm_id: "crm-1")
        @crm_id = crm_id
        @events = []
      end

      def upsert!(payload:, object_type:, match_property:, match_value:, crm_id: nil)
        @crm_id
      end

      def track_event!(**args)
        @events << args
        true
      end
    end
  end

  let(:adapter) { adapter_class.new }

  def stub_crm(adapter, match_by: {property: :ext_id, value: ->(u) { "user-#{u.id}" }})
    allow(User).to receive(:etlify_crms).and_return(
      {
        brevo: {
          adapter: adapter,
          match_by: match_by,
          crm_object_type: "contacts",
        },
      }
    )
  end

  before do
    Etlify.configure do |c|
      c.digest_strategy = Etlify::Digest.method(:stable_sha256)
    end
  end

  context "with sync_first (default)" do
    it "syncs the record, then tracks the event by crm_id",
       :aggregate_failures do
      stub_crm(adapter)

      result = described_class.call(
        user,
        crm_name: :brevo,
        name: "Inscription",
        properties: {prenom: "Jane"},
        contact_properties: {prenom: "Jane"},
        occurred_at: "2026-10-16T09:00:00Z"
      )

      expect(result).to eq(:tracked)
      expect(user.crm_synchronisations.find_by(crm_name: "brevo").crm_id)
        .to eq("crm-1")
      expect(adapter.events).to eq(
        [
          {
            name: "Inscription",
            identifier_property: "crm_id",
            identifier_value: "crm-1",
            properties: {prenom: "Jane"},
            contact_properties: {prenom: "Jane"},
            occurred_at: "2026-10-16T09:00:00Z",
          },
        ]
      )
    end

    it "uses the stored crm_id when the payload is not modified",
       :aggregate_failures do
      stub_crm(adapter)
      Etlify::Synchronizer.call(user, crm_name: :brevo)

      expect(adapter).not_to receive(:upsert!)

      described_class.call(user, crm_name: :brevo, name: "Inscription")

      expect(adapter.events.first[:identifier_value]).to eq("crm-1")
    end

    it "raises Etlify::SyncError without tracking when the sync fails",
       :aggregate_failures do
      failing = adapter_class.new
      def failing.upsert!(**)
        raise "boom"
      end
      stub_crm(failing)

      expect do
        described_class.call(user, crm_name: :brevo, name: "Inscription")
      end.to raise_error(
        Etlify::SyncError,
        "Cannot track event Inscription: sync returned :error (boom)"
      )
      expect(failing.events).to eq([])
    end

    [:skipped, :buffered].each do |result|
      it "raises Etlify::SyncError when the sync returns #{result.inspect}",
         :aggregate_failures do
        stub_crm(adapter)
        allow(Etlify::Synchronizer).to receive(:call).and_return(result)

        expect do
          described_class.call(user, crm_name: :brevo, name: "Inscription")
        end.to raise_error(
          Etlify::SyncError,
          "Cannot track event Inscription: sync returned #{result.inspect}"
        )
        expect(adapter.events).to eq([])
      end
    end

    it "raises Etlify::SyncError when the sync leaves no crm_id",
       :aggregate_failures do
      stub_crm(adapter_class.new(crm_id: nil))

      expect do
        described_class.call(user, crm_name: :brevo, name: "Inscription")
      end.to raise_error(
        Etlify::SyncError,
        "Cannot track event Inscription: sync returned :synced"
      )
    end
  end

  context "without sync_first" do
    it "tracks the event with the match_by identifier", :aggregate_failures do
      stub_crm(adapter)

      expect(adapter).not_to receive(:upsert!)

      described_class.call(
        user,
        crm_name: :brevo,
        name: "Inscription",
        sync_first: false
      )

      expect(adapter.events.first).to include(
        identifier_property: "ext_id",
        identifier_value: "user-#{user.id}"
      )
      expect(user.crm_synchronisations.find_by(crm_name: "brevo")).to be_nil
    end
  end

  context "when the CRM is disabled" do
    around do |example|
      Etlify::CRM.register(:brevo, adapter: adapter, enabled: false)
      example.run
    ensure
      Etlify::CRM.registry.delete(:brevo)
    end

    it "returns :disabled without calling the adapter", :aggregate_failures do
      stub_crm(adapter)

      result = described_class.call(user, crm_name: :brevo, name: "Inscription")

      expect(result).to eq(:disabled)
      expect(adapter.events).to eq([])
    end
  end

  context "when the adapter does not support events" do
    it "raises ArgumentError" do
      sync_only_adapter = Class.new do
        def upsert!(payload:, object_type:, match_property:, match_value:, crm_id: nil)
          "crm-1"
        end
      end
      stub_crm(sync_only_adapter.new)

      expect do
        described_class.call(user, crm_name: :brevo, name: "Inscription")
      end.to raise_error(ArgumentError, "Adapter must respond to track_event!")
    end
  end

  context "when track_event! raises" do
    it "wraps the error into Etlify::SyncError" do
      failing = adapter_class.new
      def failing.track_event!(**)
        raise Etlify::RateLimited.new("slow down", status: 429)
      end
      stub_crm(failing)

      expect do
        described_class.call(user, crm_name: :brevo, name: "Inscription")
      end.to raise_error(Etlify::SyncError, "slow down")
    end
  end
end
