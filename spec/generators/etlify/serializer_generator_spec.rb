# spec/generators/serializer_generator_spec.rb
require "rails_helper"
require "tmpdir"

RSpec.describe Etlify::Generators::SerializerGenerator, type: :generator do
  # Run a generator instance in a temp destination.
  def run_generator_in(dir, name)
    gen = described_class.new([name], {}, destination_root: dir)
    gen.invoke_all
  end

  def read(path)
    File.read(path)
  end

  describe "internal helper" do
    it "builds the expected serializer_class_name" do
      Dir.mktmpdir do |dir|
        gen = described_class.new(["admin/user"], {}, destination_root: dir)
        expect(gen.send(:serializer_class_name))
          .to eq("Admin::UserSerializer")
      end
    end
  end

  describe "template presence" do
    it "exposes a serializer template" do
      path = File.expand_path(
        "../../../lib/generators/etlify/serializer/templates/serializer.rb.tt",
        __dir__
      )
      expect(File).to exist(path)
    end
  end

  describe "generation" do
    it "creates a basic serializer under app/serializers/etlify", :aggregate_failures do
      Dir.mktmpdir do |dir|
        run_generator_in(dir, "user")

        path = File.join(
          dir,
          "app/serializers/etlify/user_serializer.rb"
        )
        expect(File).to exist(path)

        content = read(path)
        expect(content).to include("module Etlify")
        expect(content).to include("class UserSerializer")
        expect(content).to include("attr_reader :record")
        expect(content).to include("def as_crm_payload")
        expect(content).to include("id: record.id")
      end
    end

    it "respects namespaces in class_path and file layout", :aggregate_failures do
      Dir.mktmpdir do |dir|
        run_generator_in(dir, "admin/user")

        path = File.join(
          dir,
          "app/serializers/etlify/admin/user_serializer.rb"
        )
        expect(File).to exist(path)

        content = read(path)
        expect(content).to include("module Etlify")
        # NamedBase gives class_name "Admin::User"
        expect(content).to include("class Admin::UserSerializer")
        # still has the contract of BaseSerializer skeleton
        expect(content).to include("attr_reader :record")
        expect(content).to include("id: record.id")
      end
    end
  end
end
