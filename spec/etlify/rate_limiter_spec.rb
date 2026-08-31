# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::RateLimiter do
  let(:store) { ActiveSupport::Cache::MemoryStore.new }

  describe "#initialize" do
    it "computes the correct interval" do
      limiter = described_class.new(max_requests: 100, period: 10)
      expect(limiter.interval).to eq(0.1)
    end

    it "coerces the period to a float" do
      limiter = described_class.new(max_requests: 4, period: 2)
      expect(limiter.period).to eq(2.0)
    end

    it "raises on non-positive max_requests" do
      expect do
        described_class.new(max_requests: 0, period: 10)
      end.to raise_error(ArgumentError, /max_requests/)
    end

    it "raises on non-numeric max_requests" do
      expect do
        described_class.new(max_requests: "5", period: 10)
      end.to raise_error(ArgumentError, /max_requests/)
    end

    it "raises on non-positive period" do
      expect do
        described_class.new(max_requests: 10, period: -1)
      end.to raise_error(ArgumentError, /period/)
    end

    it "raises on non-numeric period" do
      expect do
        described_class.new(max_requests: 10, period: "1")
      end.to raise_error(ArgumentError, /period/)
    end

    it "raises when the cache cannot increment" do
      expect do
        described_class.new(max_requests: 10, period: 1, cache: Object.new)
      end.to raise_error(ArgumentError, /#increment/)
    end

    it "points to the false opt-out in the cache error message" do
      expect do
        described_class.new(max_requests: 10, period: 1, cache: Object.new)
      end.to raise_error(ArgumentError, /cache: false/)
    end

    it "defaults the key namespace" do
      limiter = described_class.new(max_requests: 10, period: 1)
      expect(limiter.key).to eq(described_class::DEFAULT_KEY)
    end

    it "keeps an explicit key" do
      limiter = described_class.new(
        max_requests: 10,
        period: 1,
        key: "etlify:rate_limit:hubspot"
      )
      expect(limiter.key).to eq("etlify:rate_limit:hubspot")
    end
  end

  describe "#shared?" do
    it "is false without a cache" do
      limiter = described_class.new(max_requests: 10, period: 1)
      expect(limiter).not_to be_shared
    end

    it "is false when the cache is explicitly nil" do
      limiter = described_class.new(max_requests: 10, period: 1, cache: nil)
      expect(limiter).not_to be_shared
    end

    it "is false when the bucket is explicitly disabled" do
      limiter = described_class.new(max_requests: 10, period: 1, cache: false)
      expect(limiter).not_to be_shared
    end

    it "is true with a cache" do
      limiter = described_class.new(max_requests: 10, period: 1, cache: store)
      expect(limiter).to be_shared
    end
  end

  describe "#throttle! without a cache" do
    it "sleeps for the configured interval on each call" do
      limiter = described_class.new(max_requests: 10, period: 1)

      allow(limiter).to receive(:sleep)

      limiter.throttle!
      limiter.throttle!

      expect(limiter).to have_received(:sleep).with(0.1).twice
    end

    it "never touches the store" do
      limiter = described_class.new(max_requests: 10, period: 1, cache: false)
      allow(limiter).to receive(:sleep)
      allow(store).to receive(:increment)

      limiter.throttle!

      expect(store).not_to have_received(:increment)
    end
  end

  describe "#throttle! with a shared bucket" do
    subject(:limiter) do
      described_class.new(
        max_requests: 3,
        period: 1,
        cache: store,
        key: "etlify:rate_limit:test"
      )
    end

    # The window is derived from wall-clock time. Pinning it keeps the
    # examples deterministic instead of depending on when they happen to run.
    before do
      allow(limiter).to receive(:sleep)
      allow(limiter).to receive(:current_time).and_return(0.0)
    end

    it "lets calls through without sleeping while budget remains" do
      3.times { limiter.throttle! }

      expect(limiter).not_to have_received(:sleep)
    end

    it "claims one slot per call in the current window" do
      2.times { limiter.throttle! }

      expect(store.read("etlify:rate_limit:test:0")).to eq(2)
    end

    it "waits for the next window once the budget is spent" do
      allow(limiter).to receive(:current_time)
        .and_return(0.0, 0.0, 0.0, 0.0, 1.0)

      4.times { limiter.throttle! }

      expect(limiter).to have_received(:sleep).once
    end

    it "sleeps until the next window boundary, plus jitter" do
      allow(limiter).to receive(:current_time)
        .and_return(0.25, 0.25, 0.25, 0.25, 1.0)

      4.times { limiter.throttle! }

      # 0.75s left in the window, plus a jitter below one interval.
      expect(limiter).to have_received(:sleep) do |duration|
        expect(duration).to be_between(0.75, 0.75 + limiter.interval)
      end
    end

    it "starts a fresh budget in the next window" do
      allow(limiter).to receive(:current_time).and_return(0.0, 1.0)

      2.times { limiter.throttle! }

      expect(store.read("etlify:rate_limit:test:0")).to eq(1)
      expect(store.read("etlify:rate_limit:test:1")).to eq(1)
    end

    it "does not share a budget with another key" do
      other = described_class.new(
        max_requests: 3,
        period: 1,
        cache: store,
        key: "etlify:rate_limit:other"
      )
      allow(other).to receive(:sleep)
      allow(other).to receive(:current_time).and_return(0.0)

      3.times { limiter.throttle! }
      3.times { other.throttle! }

      expect(limiter).not_to have_received(:sleep)
      expect(other).not_to have_received(:sleep)
    end

    it "expires the window key so counters cannot leak" do
      allow(store).to receive(:increment).and_return(1)

      limiter.throttle!

      expect(store).to have_received(:increment).with(
        "etlify:rate_limit:test:0",
        1,
        expires_in: 2
      )
    end

    it "falls back to local pacing when the store cannot answer" do
      allow(store).to receive(:increment).and_return(nil)

      limiter.throttle!

      expect(limiter).to have_received(:sleep).with(limiter.interval).once
    end

    it "falls back to local pacing when the store raises" do
      allow(store).to receive(:increment).and_raise(RuntimeError, "boom")

      expect { limiter.throttle! }.not_to raise_error
      expect(limiter).to have_received(:sleep).with(limiter.interval).once
    end

    it "gives up waiting after MAX_WINDOW_WAITS windows" do
      allow(store).to receive(:increment).and_return(99)

      limiter.throttle!

      expect(limiter).to have_received(:sleep).exactly(
        described_class::MAX_WINDOW_WAITS + 1
      ).times
    end

    it "paces the call it finally lets through after giving up" do
      allow(store).to receive(:increment).and_return(99)

      limiter.throttle!

      expect(limiter).to have_received(:sleep).with(limiter.interval).once
    end
  end

  describe "#throttle! window derivation" do
    it "derives the window from the wall clock" do
      limiter = described_class.new(
        max_requests: 3,
        period: 1,
        cache: store,
        key: "etlify:rate_limit:clock"
      )
      allow(limiter).to receive(:sleep)

      limiter.throttle!

      # The call may land either side of a second boundary, so accept the
      # neighbouring windows rather than pinning the exact one.
      window = (Time.now.to_f / 1.0).floor
      counted = [window - 1, window, window + 1].filter_map do |candidate|
        store.read("etlify:rate_limit:clock:#{candidate}")
      end

      expect(counted).to eq([1])
    end
  end

  describe Etlify::RateLimiter::NullLimiter do
    it "does not sleep" do
      limiter = described_class.new

      expect(limiter).not_to receive(:sleep)
      limiter.throttle!
      limiter.throttle!
    end

    it "returns 0 for interval" do
      expect(described_class.new.interval).to eq(0)
    end

    it "is never shared" do
      expect(described_class.new).not_to be_shared
    end
  end
end
