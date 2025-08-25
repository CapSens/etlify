# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::Digest do
  describe ".normalize" do
    it "sorts hash keys recursively and preserves array order",
       :aggregate_failures do
      # Hash keys (including nested) are sorted; arrays keep their order.
      input = {
        b: 2,
        a: {
          z: [3, {k: 1}],
          c: 1,
        },
      }

      out = described_class.normalize(input)

      expect(out.keys).to eq(%i[a b])
      expect(out[:a].keys).to eq(%i[c z])
      expect(out[:a][:z]).to eq([3, {k: 1}])
    end

    it "recursively normalizes arrays of mixed types",
       :aggregate_failures do
      input = [
        {b: 1, a: 2},
        3,
        [
          {d: 4, c: 5},
          6,
        ],
      ]

      out = described_class.normalize(input)

      expect(out).to eq(
        [
          {a: 2, b: 1},
          3,
          [
            {c: 5, d: 4},
            6,
          ],
        ]
      )
    end

    it "returns scalars unchanged (String, Numeric, booleans, nil)",
       :aggregate_failures do
      expect(described_class.normalize("x")).to eq("x")
      expect(described_class.normalize(42)).to eq(42)
      expect(described_class.normalize(true)).to eq(true)
      expect(described_class.normalize(false)).to eq(false)
      expect(described_class.normalize(nil)).to be_nil
    end
  end

  describe ".stable_sha256" do
    it "returns a deterministic 64-char lowercase hex digest",
       :aggregate_failures do
      payload = {
        a: 1,
        b: [
          2,
          3,
          {c: 4},
        ],
      }

      d1 = described_class.stable_sha256(payload)
      d2 = described_class.stable_sha256(payload)

      expect(d1).to match(/\A\h{64}\z/)
      expect(d1).to eq(d2)
    end

    it "is insensitive to hash key ordering (shallow and nested)",
       :aggregate_failures do
      p1 = {a: 1, b: 2}
      p2 = {b: 2, a: 1}

      expect(described_class.stable_sha256(p1))
        .to eq(described_class.stable_sha256(p2))

      n1 = {
        x: {m: 1, n: 2},
        y: [3, 4],
      }
      n2 = {
        x: {n: 2, m: 1},
        y: [3, 4],
      }

      expect(described_class.stable_sha256(n1))
        .to eq(described_class.stable_sha256(n2))
    end

    it "changes when a value changes",
       :aggregate_failures do
      p1 = {
        a: 1,
        b: [2, 3],
      }
      p2 = {
        a: 1,
        b: [2, 4],
      }

      expect(described_class.stable_sha256(p1))
        .not_to eq(described_class.stable_sha256(p2))
    end

    it "is sensitive to array element order",
       :aggregate_failures do
      p1 = {a: [1, 2, 3]}
      p2 = {a: [3, 2, 1]}

      expect(described_class.stable_sha256(p1))
        .not_to eq(described_class.stable_sha256(p2))
    end

    it "handles primitive inputs (String, Numeric, booleans, nil)",
       :aggregate_failures do
      expect(described_class.stable_sha256("hello")).to match(/\A\h{64}\z/)
      expect(described_class.stable_sha256(123)).to match(/\A\h{64}\z/)
      expect(described_class.stable_sha256(true)).to match(/\A\h{64}\z/)
      expect(described_class.stable_sha256(false)).to match(/\A\h{64}\z/)
      expect(described_class.stable_sha256(nil)).to match(/\A\h{64}\z/)
    end

    it "matches for deeply equivalent structures regardless of key order",
       :aggregate_failures do
      p1 = {
        a: [
          {z: 1, y: 2},
          {c: [3, {b: 4, a: 5}]},
        ],
        k: 9,
      }
      p2 = {
        k: 9,
        a: [
          {y: 2, z: 1},
          {c: [3, {a: 5, b: 4}]},
        ],
      }

      expect(described_class.stable_sha256(p1))
        .to eq(described_class.stable_sha256(p2))
    end
  end
end
