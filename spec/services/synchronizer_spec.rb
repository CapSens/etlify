describe Etlify::Synchronizer do
  include_context "with companies and users"

  describe ".call with adapter args and memoization" do
    it(
      "passes object_type, id_property, payload and memoizes payload",
      :aggregate_failures
    ) do
      # Spy digest strategy to ensure it is called with the memoized payload
      strategy = double("DigestStrategy")
      allow(strategy).to receive(:call).and_return("digest-123")
      allow(Etlify.config).to receive(:digest_strategy).and_return(strategy)

      # Force build_crm_payload to be called only once (memoization)
      # Then let the real implementation build a valid payload.
      expect(user).to receive(:build_crm_payload).once.and_call_original

      # Ensure we pass exact args to upsert! including crm_id:nil on first run
      adapter = instance_double("Adapter")
      expect(adapter).to receive(:upsert!).with(
        hash_including(
          object_type: "contacts",
          id_property: :id,
          payload: kind_of(Hash),
          crm_id: nil
        )
      ).and_return("crm-111")
      allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

      result = described_class.call(user)

      expect(result).to eq(:synced)
      expect(strategy).to have_received(:call).with(kind_of(Hash)).twice
      expect(user.reload.crm_synchronisation.crm_id).to eq("crm-111")
    end
  end

  describe ".call when crm_id already present" do
    it(
      "does not overwrite crm_id even if adapter returns a different id",
      :aggregate_failures
    ) do
      user.create_crm_synchronisation!(crm_id: "crm-old")

      adapter = instance_double("Adapter")
      # Adapter returns a different id; synchronizer must not override.
      expect(adapter).to receive(:upsert!).with(
        hash_including(crm_id: "crm-old")
      ).and_return("crm-new")
      allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

      described_class.call(user)

      sync = user.reload.crm_synchronisation
      expect(sync.crm_id).to eq("crm-old")
      expect(sync.last_error).to be_nil
      expect(sync.last_digest).to be_present
      expect(sync.last_synced_at).to be_present
    end
  end

  describe ".call when adapter returns blank id" do
    it(
      "keeps crm_id nil when adapter returns blank",
      :aggregate_failures
    ) do
      adapter = instance_double("Adapter")
      expect(adapter).to receive(:upsert!).with(
        hash_including(crm_id: nil)
      ).and_return("") # blank ⇒ presence => nil
      allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

      described_class.call(user)

      sync = user.reload.crm_synchronisation
      expect(sync.crm_id).to be_nil
      expect(sync.last_error).to be_nil
      expect(sync.last_digest).to be_present
      expect(sync.last_synced_at).to be_present
    end
  end

  describe ".call when not stale" do
    it(
      "does not call upsert! and only touches last_synced_at",
      :aggregate_failures
    ) do
      # First run to create sync row with a known digest
      ok_adapter = instance_double("Adapter", upsert!: "crm-222")
      allow(Etlify.config).to receive(:crm_adapter).and_return(ok_adapter)
      described_class.call(user)

      # Force stale? to be false to hit the :not_modified branch
      allow_any_instance_of(CrmSynchronisation).to(
        receive(:stale?).and_return(false)
      )

      # No upsert! should happen on the second call
      expect(Etlify.config.crm_adapter).not_to receive(:upsert!)

      prev_synced_at = user.reload.crm_synchronisation.last_synced_at
      Timecop.freeze(prev_synced_at + 2.seconds) do
        result = described_class.call(user)
        expect(result).to eq(:not_modified)
        expect(user.reload.crm_synchronisation.last_synced_at)
          .to eq(prev_synced_at + 2.seconds)
      end
    end
  end

  describe ".call error handling with StandardError" do
    it(
      "stores the error message and does not change prior success state",
      :aggregate_failures
    ) do
      # First succeed to set a valid state
      ok_adapter = instance_double("Adapter", upsert!: "crm-ok")
      allow(Etlify.config).to receive(:crm_adapter).and_return(ok_adapter)
      described_class.call(user)

      # Next run raises a generic StandardError
      failing = instance_double("Adapter")
      allow(failing).to receive(:upsert!).and_raise(StandardError.new("boom"))
      allow(Etlify.config).to receive(:crm_adapter).and_return(failing)

      # Force the stale? branch so upsert! is invoked and error is rescued
      allow_any_instance_of(CrmSynchronisation).to(
        receive(:stale?).and_return(true)
      )

      expect { described_class.call(user) }.not_to raise_error

      sync = user.reload.crm_synchronisation
      expect(sync.last_error).to eq("boom")
      # Ensure previous success fields remain intact
      expect(sync.crm_id).to eq("crm-ok")
      expect(sync.last_digest).to be_present
      expect(sync.last_synced_at).to be_present
    end
  end

  describe ".call locking and line creation" do
    it(
      "wraps in with_lock and builds the sync line if missing",
      :aggregate_failures
    ) do
      # Fresh user: no sync row yet
      expect(user.crm_synchronisation).to be_nil

      adapter = instance_double("Adapter", upsert!: "crm-333")
      allow(Etlify.config).to receive(:crm_adapter).and_return(adapter)

      # Ensure with_lock is actually used around the critical section
      expect(user).to receive(:with_lock).and_call_original

      described_class.call(user)

      sync = user.reload.crm_synchronisation
      expect(sync).to be_present
      expect(sync.crm_id).to eq("crm-333")
      expect(sync.last_digest).to be_present
      expect(sync.last_synced_at).to be_present
    end
  end
end
