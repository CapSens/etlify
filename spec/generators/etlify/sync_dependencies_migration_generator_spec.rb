# frozen_string_literal: true

require "rails_helper"
require "fileutils"
require "tmpdir"

RSpec.describe Etlify::Generators::SyncDependenciesMigrationGenerator,
               type: :generator do
  def build_generator(args)
    described_class.new(
      args,
      {},
      destination_root: @tmp_dir
    )
  end

  def find_migration_by_suffix(suffix)
    Dir[File.join(@tmp_dir, "db/migrate/*_#{suffix}")].first
  end

  around do |example|
    Dir.mktmpdir do |dir|
      @tmp_dir = dir
      Dir.chdir(@tmp_dir) do
        FileUtils.mkdir_p("db/migrate")
        example.run
      end
    end
  end

  describe "#copy_migration (default filename)" do
    it "creates a timestamped migration using the default name",
       :aggregate_failures do
      gen = build_generator([""])
      gen.invoke_all

      path = find_migration_by_suffix(
        "create_etlify_sync_dependencies.rb"
      )
      expect(path).to be_a(String)
      expect(File.exist?(path)).to eq(true)

      content = File.read(path)
      expect(content).to match(
        /class CreateEtlifySyncDependencies < ActiveRecord::Migration\[\d+\.\d+\]/
      )
    end
  end

  describe "#copy_migration (custom filename)" do
    it "creates a timestamped migration using the provided name",
       :aggregate_failures do
      gen = build_generator(["add_sync_deps"])
      gen.invoke_all

      path = find_migration_by_suffix("add_sync_deps.rb")
      expect(path).to be_a(String)
      expect(File.exist?(path)).to eq(true)

      content = File.read(path)
      expect(content).to include(
        "class AddSyncDeps < ActiveRecord::Migration"
      )
    end
  end

  describe "template content" do
    it "contains the expected columns and indexes",
       :aggregate_failures do
      gen = build_generator([""])
      gen.invoke_all

      path = find_migration_by_suffix(
        "create_etlify_sync_dependencies.rb"
      )
      content = File.read(path)

      # Table
      expect(content).to include(
        "create_table :etlify_sync_dependencies"
      )

      # Columns
      expect(content).to include(
        "t.string  :resource_type,        null: false"
      )
      expect(content).to include(
        "t.bigint  :resource_id,          null: false"
      )
      expect(content).to include(
        "t.string  :parent_resource_type, null: false"
      )
      expect(content).to include(
        "t.bigint  :parent_resource_id,   null: false"
      )
      expect(content).to include(
        "t.string  :crm_name,             null: false"
      )

      # Indexes
      expect(content).to include(
        'name: "idx_sync_deps_on_parent"'
      )
      expect(content).to include(
        'name: "idx_sync_deps_on_child"'
      )
      expect(content).to include(
        'name: "idx_sync_deps_unique"'
      )
    end
  end

  describe "private helpers" do
    it "file_name returns default when name is blank" do
      gen = build_generator([""])
      expect(gen.send(:file_name)).to eq(
        described_class::DEFAULT_MIGRATION_FILENAME
      )
    end

    it "file_name underscores a provided CamelCase name" do
      gen = build_generator(["AddSyncDeps"])
      expect(gen.send(:file_name)).to eq("add_sync_deps")
    end
  end

  describe ".next_migration_number" do
    it "returns a UTC timestamp (YYYYMMDDHHMMSS)" do
      fixed = Time.utc(2026, 2, 20, 10, 30, 0)
      allow(Time).to receive(:now).and_return(fixed)

      n = described_class.next_migration_number("ignored")
      expect(n).to match(/\A\d{14}\z/)
      expect(n).to eq("20260220103000")
    end
  end
end
