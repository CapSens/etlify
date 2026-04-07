require "rails_helper"
require "etlify/adapters/airtable_v0/formula"

RSpec.describe Etlify::Adapters::AirtableV0::Formula do
  describe ".eq" do
    it "builds an equality formula for a string value" do
      result = described_class.eq("Email", "john@example.com")
      expect(result).to eq('{Email} = "john@example.com"')
    end

    it "builds an equality formula for a numeric value" do
      result = described_class.eq("Age", 25)
      expect(result).to eq("{Age} = 25")
    end

    it "rejects field names with special characters" do
      expect do
        described_class.eq("} = 1) OR 1=1; //", "hack")
      end.to raise_error(
        ArgumentError, /Invalid Airtable field name/
      )
    end

    it "rejects non-scalar values" do
      expect do
        described_class.eq("Email", ["a@b.com"])
      end.to raise_error(
        ArgumentError, /Formula value must be/
      )
    end
  end

  describe ".escape" do
    it "quotes strings with double quotes" do
      expect(described_class.escape("hello")).to eq('"hello"')
    end

    it "leaves numerics unquoted" do
      expect(described_class.escape(42)).to eq("42")
    end

    it "escapes backslashes in string values" do
      result = described_class.escape('back\\slash')
      expect(result).to eq('"back\\\\slash"')
    end

    it "escapes double quotes in string values" do
      result = described_class.escape('say "hi"')
      expect(result).to eq('"say \\"hi\\""')
    end

    it "accepts symbols" do
      expect(described_class.escape(:test)).to eq('"test"')
    end

    it "raises on non-scalar types" do
      expect do
        described_class.escape([1, 2])
      end.to raise_error(ArgumentError, /Formula value must be/)
    end
  end
end
