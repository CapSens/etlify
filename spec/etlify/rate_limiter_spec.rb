# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::RateLimiter do
  describe "#initialize" do
    it "computes the correct interval" do
      limiter = described_class.new(max_requests: 100, period: 10)
      expect(limiter.interval).to eq(0.1)
    end

    it "raises on non-positive max_requests" do
      expect do
        described_class.new(max_requests: 0, period: 10)
      end.to raise_error(ArgumentError, /max_requests/)
    end

    it "raises on non-positive period" do
      expect do
        described_class.new(max_requests: 10, period: -1)
      end.to raise_error(ArgumentError, /period/)
    end
  end

  describe "#throttle!" do
    it "does not sleep on the first call" do
      limiter = described_class.new(max_requests: 10, period: 1)

      expect(limiter).not_to receive(:sleep)
      limiter.throttle!
    end

    it "sleeps the required amount between calls" do
      limiter = described_class.new(max_requests: 10, period: 1)
      # interval = 0.1s

      allow(limiter).to receive(:sleep)
      # Simulate immediate second call (no elapsed time)
      allow(limiter).to receive(:monotonic_now).and_return(100.0, 100.0)

      limiter.throttle!
      limiter.throttle!

      expect(limiter).to have_received(:sleep).with(0.1).once
    end

    it "does not sleep when enough time has elapsed" do
      limiter = described_class.new(max_requests: 10, period: 1)

      allow(limiter).to receive(:sleep)
      # 0.2s elapsed, interval is 0.1s -> no sleep needed
      allow(limiter).to receive(:monotonic_now).and_return(100.0, 100.2)

      limiter.throttle!
      limiter.throttle!

      expect(limiter).not_to have_received(:sleep)
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
  end
end
