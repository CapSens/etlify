require "rails_helper"

RSpec.describe Etlify::CRM do
  # Minimal adapter instance used for registration
  let(:adapter_instance) do
    Class.new do
      def upsert!(**)
      end
    end.new
  end

  before do
    # Reset registry between examples to avoid cross-test pollution.
    # We rely on the public API to clear state.
    described_class.registry.clear
  end

  it "registers CRMs and exposes names + fetch" do
    described_class.register(
      :hubspot,
      adapter: adapter_instance,
      options: {job_class: "X"}
    )

    item = described_class.fetch(:hubspot)

    expect(item.name).to eq(:hubspot)
    expect(item.adapter).to eq(adapter_instance)
    expect(item.options[:job_class]).to eq("X")
    expect(described_class.names).to include(:hubspot)
  end

  it "normalizes registry keys to symbols" do
    described_class.register("salesforce", adapter: adapter_instance)

    expect(described_class.names).to include(:salesforce)
    expect(described_class.fetch(:salesforce).name).to eq(:salesforce)
  end

  it "raises when adapter is a class (must be an instance)" do
    klass = Class.new do
  def upsert!(**)
  end
end

    expect do
      described_class.register(:bad, adapter: klass)
    end.to raise_error(
      ArgumentError,
      "Adapter must be an instance, not a class"
    )
  end

  it "does not mutate given options and stores a copy" do
    opts = {job_class: "X"}
    described_class.register(
      :pipedrive,
      adapter: adapter_instance,
      options: opts
    )

    opts[:job_class] = "Y" # mutate original hash

    item = described_class.fetch(:pipedrive)
    expect(item.options[:job_class]).to eq("X")
  end

  it "supports multiple registrations and keeps them independent" do
    a1 = Class.new do
  def upsert!(**)
  end
end.new
    a2 = Class.new do
  def upsert!(**)
  end
end.new

    described_class.register(:hubspot, adapter: a1, options: {job: "A"})
    described_class.register(:zoho, adapter: a2, options: {job: "B"})

    hubspot = described_class.fetch(:hubspot)
    zoho = described_class.fetch(:zoho)

    expect(hubspot.adapter).to eq(a1)
    expect(zoho.adapter).to eq(a2)
    expect(hubspot.options[:job]).to eq("A")
    expect(zoho.options[:job]).to eq("B")
    expect(described_class.names).to match_array([:hubspot, :zoho])
  end

  it "fetch raises when CRM is not registered" do
    expect { described_class.fetch(:unknown) }.to raise_error(KeyError)
  end

  describe "enabled flag" do
    it "defaults to true when not provided" do
      described_class.register(:hubspot, adapter: adapter_instance)

      expect(described_class.fetch(:hubspot).enabled).to be(true)
      expect(described_class.enabled?(:hubspot)).to be(true)
    end

    it "stores enabled: false and exposes it via enabled?" do
      described_class.register(
        :hubspot,
        adapter: adapter_instance,
        enabled: false
      )

      expect(described_class.fetch(:hubspot).enabled).to be(false)
      expect(described_class.enabled?(:hubspot)).to be(false)
    end

    it "accepts enabled: true explicitly" do
      described_class.register(
        :hubspot,
        adapter: adapter_instance,
        enabled: true
      )

      expect(described_class.enabled?(:hubspot)).to be(true)
    end

    it "raises when enabled is not a boolean" do
      expect do
        described_class.register(
          :hubspot,
          adapter: adapter_instance,
          enabled: "yes"
        )
      end.to raise_error(
        ArgumentError,
        "enabled must be a boolean (true or false)"
      )
    end

    it "returns true for unknown CRMs (safe default)" do
      expect(described_class.enabled?(:unknown)).to be(true)
    end
  end

  describe "rate limiter installation" do
    let(:throttled_adapter) do
      Class.new do
        attr_accessor :rate_limiter

        def upsert!(**)
        end
      end.new
    end

    let(:store) { ActiveSupport::Cache::MemoryStore.new }

    it "installs no limiter when no rate_limit is configured" do
      described_class.register(:hubspot, adapter: throttled_adapter)

      expect(throttled_adapter.rate_limiter).to be_nil
    end

    it "skips adapters without a rate_limiter accessor" do
      expect do
        described_class.register(
          :hubspot,
          adapter: adapter_instance,
          options: {rate_limit: {max_requests: 5, period: 1}}
        )
      end.not_to raise_error

      expect(adapter_instance).not_to respond_to(:rate_limiter)
    end

    it "installs a limiter with the configured rate" do
      described_class.register(
        :hubspot,
        adapter: throttled_adapter,
        options: {rate_limit: {max_requests: 5, period: 1}}
      )

      limiter = throttled_adapter.rate_limiter

      expect(limiter).to be_a(Etlify::RateLimiter)
      expect(limiter.max_requests).to eq(5)
      expect(limiter.period).to eq(1.0)
    end

    it "scopes the bucket key per CRM" do
      described_class.register(
        :hubspot,
        adapter: throttled_adapter,
        options: {rate_limit: {max_requests: 5, period: 1}}
      )

      expect(throttled_adapter.rate_limiter.key).to eq(
        "#{Etlify::RateLimiter::DEFAULT_KEY}:hubspot"
      )
    end

    it "shares the bucket through the configured cache store by default" do
      allow(Etlify.config).to receive(:cache_store).and_return(store)

      described_class.register(
        :hubspot,
        adapter: throttled_adapter,
        options: {rate_limit: {max_requests: 5, period: 1}}
      )

      expect(throttled_adapter.rate_limiter).to be_shared
    end

    it "honours an explicit cache store" do
      allow(Etlify.config).to receive(:cache_store).and_return(nil)

      described_class.register(
        :hubspot,
        adapter: throttled_adapter,
        options: {
          rate_limit: {max_requests: 5, period: 1, cache: store},
        }
      )

      expect(throttled_adapter.rate_limiter).to be_shared
    end

    it "disables the bucket on cache: false" do
      allow(Etlify.config).to receive(:cache_store).and_return(store)

      described_class.register(
        :hubspot,
        adapter: throttled_adapter,
        options: {
          rate_limit: {max_requests: 5, period: 1, cache: false},
        }
      )

      expect(throttled_adapter.rate_limiter).not_to be_shared
    end

    it "disables the bucket when the configured store is nil" do
      allow(Etlify.config).to receive(:cache_store).and_return(nil)

      described_class.register(
        :hubspot,
        adapter: throttled_adapter,
        options: {rate_limit: {max_requests: 5, period: 1}}
      )

      expect(throttled_adapter.rate_limiter).not_to be_shared
    end

    it "raises when rate_limit is not a Hash" do
      expect do
        described_class.register(
          :hubspot,
          adapter: throttled_adapter,
          options: {rate_limit: "5 per second"}
        )
      end.to raise_error(ArgumentError, "rate_limit must be a Hash")
    end

    it "raises when max_requests is missing" do
      expect do
        described_class.register(
          :hubspot,
          adapter: throttled_adapter,
          options: {rate_limit: {period: 1}}
        )
      end.to raise_error(
        ArgumentError,
        "rate_limit[:max_requests] must be a positive number"
      )
    end

    it "raises when max_requests is not positive" do
      expect do
        described_class.register(
          :hubspot,
          adapter: throttled_adapter,
          options: {rate_limit: {max_requests: 0, period: 1}}
        )
      end.to raise_error(
        ArgumentError,
        "rate_limit[:max_requests] must be a positive number"
      )
    end

    it "raises when period is missing" do
      expect do
        described_class.register(
          :hubspot,
          adapter: throttled_adapter,
          options: {rate_limit: {max_requests: 5}}
        )
      end.to raise_error(
        ArgumentError,
        "rate_limit[:period] must be a positive number"
      )
    end

    it "raises when period is not positive" do
      expect do
        described_class.register(
          :hubspot,
          adapter: throttled_adapter,
          options: {rate_limit: {max_requests: 5, period: 0}}
        )
      end.to raise_error(
        ArgumentError,
        "rate_limit[:period] must be a positive number"
      )
    end

    it "raises at registration when the cache cannot increment" do
      expect do
        described_class.register(
          :hubspot,
          adapter: throttled_adapter,
          options: {
            rate_limit: {max_requests: 5, period: 1, cache: Object.new},
          }
        )
      end.to raise_error(ArgumentError, /#increment/)
    end
  end
end
