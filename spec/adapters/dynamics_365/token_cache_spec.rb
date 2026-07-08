require "rails_helper"
require "etlify/adapters/dynamics_365/token_cache"

RSpec.describe Etlify::Adapters::Dynamics365::TokenCache do
  let(:now) { Time.utc(2026, 4, 27, 12, 0, 0) }
  let(:clock) { -> { now } }

  subject(:cache) { described_class.new(clock: clock) }

  describe "#fetch" do
    it "calls the block on first call" do
      calls = 0
      token = cache.fetch do
        calls += 1
        {token: "abc", expires_at: now + 3600}
      end

      expect(token).to eq("abc")
      expect(calls).to eq(1)
    end

    it "does not call the block when the entry is still fresh" do
      cache.fetch { {token: "abc", expires_at: now + 3600} }

      calls = 0
      token = cache.fetch do
        calls += 1
        raise "should not be called"
      end

      expect(token).to eq("abc")
      expect(calls).to eq(0)
    end

    it "calls the block again when the entry is past its safety margin" do
      cache.fetch { {token: "old", expires_at: now + 30} }

      token = cache.fetch { {token: "new", expires_at: now + 3600} }

      expect(token).to eq("new")
    end

    it "calls the block again when the entry is exactly at the safety margin" do
      margin = described_class::SAFETY_MARGIN_SECONDS
      cache.fetch { {token: "old", expires_at: now + margin} }

      token = cache.fetch { {token: "new", expires_at: now + 3600} }

      expect(token).to eq("new")
    end

    it "raises if the block returns an invalid entry" do
      expect do
        cache.fetch { {token: nil, expires_at: now + 3600} }
      end.to raise_error(ArgumentError, /TokenCache block/)
    end

    it "raises if the block returns a non-Hash" do
      expect do
        cache.fetch { "not a hash" }
      end.to raise_error(ArgumentError, /TokenCache block/)
    end
  end

  describe "#invalidate!" do
    it "forces the next fetch to call the block again" do
      cache.fetch { {token: "abc", expires_at: now + 3600} }
      cache.invalidate!

      token = cache.fetch { {token: "def", expires_at: now + 3600} }

      expect(token).to eq("def")
    end
  end

  describe "thread safety" do
    it "calls the block exactly once when many threads hit a cold cache" do
      mutex = Mutex.new
      call_count = 0

      block = proc do
        mutex.synchronize { call_count += 1 }
        sleep(0.01)
        {token: "shared", expires_at: now + 3600}
      end

      threads = Array.new(20) do
        Thread.new { cache.fetch(&block) }
      end
      results = threads.map(&:value)

      expect(results.uniq).to eq(["shared"])
      expect(call_count).to eq(1)
    end
  end
end
