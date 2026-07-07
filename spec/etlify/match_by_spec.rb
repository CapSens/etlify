require "rails_helper"

RSpec.describe Etlify::MatchBy do
  describe ".validate!" do
    it "returns the hash when valid with a Symbol value" do
      match_by = {property: :email, value: :email}
      expect(described_class.validate!(match_by)).to eq(match_by)
    end

    it "returns the hash when valid with a Proc value" do
      match_by = {property: "email", value: ->(r) { r.email }}
      expect(described_class.validate!(match_by)).to eq(match_by)
    end

    it "raises when match_by is not a Hash" do
      expect { described_class.validate!(:email) }.to(
        raise_error(ArgumentError, /match_by must be a Hash/)
      )
    end

    it "raises when property is missing, blank or not a name" do
      [
        {value: :email},
        {property: "", value: :email},
        {property: "   ", value: :email},
        {property: 42, value: :email},
      ].each do |match_by|
        expect { described_class.validate!(match_by) }.to(
          raise_error(ArgumentError, /match_by\[:property\]/)
        )
      end
    end

    it "raises when value is neither a Proc nor a Symbol" do
      [
        {property: :email},
        {property: :email, value: "email"},
        {property: :email, value: 42},
      ].each do |match_by|
        expect { described_class.validate!(match_by) }.to(
          raise_error(ArgumentError, /match_by\[:value\]/)
        )
      end
    end
  end

  describe ".property" do
    it "returns the property as a String" do
      conf = {match_by: {property: :email, value: :email}}
      expect(described_class.property(conf)).to eq("email")
    end
  end

  describe ".resolve" do
    let(:resource) { Struct.new(:email).new("  John@Example.com ") }

    it "calls the Proc with the resource and strips the result" do
      conf = {match_by: {property: :email, value: ->(r) { r.email }}}
      expect(described_class.resolve(resource, conf)).to(
        eq("John@Example.com")
      )
    end

    it "sends the Symbol to the resource and strips the result" do
      conf = {match_by: {property: :email, value: :email}}
      expect(described_class.resolve(resource, conf)).to(
        eq("John@Example.com")
      )
    end

    it "returns an empty string when the source value is nil" do
      conf = {match_by: {property: :email, value: ->(_r) {}}}
      expect(described_class.resolve(resource, conf)).to eq("")
    end

    it "stringifies non-string source values" do
      conf = {match_by: {property: :swc_uuid, value: ->(_r) { 42 }}}
      expect(described_class.resolve(resource, conf)).to eq("42")
    end

    it "propagates NoMethodError for an unknown Symbol" do
      conf = {match_by: {property: :email, value: :unknown_method}}
      expect do
        described_class.resolve(resource, conf)
      end.to raise_error(NoMethodError)
    end
  end
end
