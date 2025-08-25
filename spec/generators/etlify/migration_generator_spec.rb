# frozen_string_literal: true

require "rails_helper"
require "fileutils"
require "tmpdir"

RSpec.describe Etlify::Generators::MigrationGenerator, type: :generator do
  # Build a fresh generator instance targeting a temp destination.
  def build_generator(args)
    described_class.new(
      args,
      {},
      destination_root: @tmp_dir
    )
  end

  # Find the single generated migration file by a suffix pattern.
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
      # Thor requires an argument; pass "" to trigger presence ⇒ nil
      gen = build_generator([""])
      gen.invoke_all

      path = find_migration_by_suffix("create_crm_synchronisations.rb")
      expect(path).to be_a(String)
      expect(File.exist?(path)).to eq(true)

      content = File.read(path)
      expect(content).to match(
        /class CreateCrmSynchronisations < ActiveRecord::Migration\[\d+\.\d+\]/
      )
      major = ActiveRecord::VERSION::MAJOR
      minor = ActiveRecord::VERSION::MINOR
      expect(content).to include(
        "ActiveRecord::Migration[#{major}.#{minor}]"
      )
    end
  end

  describe "#copy_migration (custom filename)" do
    it "creates a timestamped migration using the provided name",
       :aggregate_failures do
      gen = build_generator(["add_foo_bar"])
      gen.invoke_all

      path = find_migration_by_suffix("add_foo_bar.rb")
      expect(path).to be_a(String)
      expect(File.exist?(path)).to eq(true)

      content = File.read(path)
      expect(content).to include(
        "class AddFooBar < ActiveRecord::Migration"
      )
    end
  end

  describe "template content" do
    it "contains the expected columns and indexes",
       :aggregate_failures do
      gen = build_generator([""])
      gen.invoke_all

      path = find_migration_by_suffix("create_crm_synchronisations.rb")
      content = File.read(path)

      # Columns
      expect(content).to include("t.string   :crm_id")
      expect(content).to include(
        "t.string   :resource_type, null: false"
      )
      expect(content).to include(
        "t.bigint   :resource_id,   null: false"
      )
      expect(content).to include("t.string   :last_digest")
      expect(content).to include("t.datetime :last_synced_at")
      expect(content).to include("t.string   :last_error")
      expect(content).to include("t.string   :crm")

      # Indexes (assert literal content from template)
      expect(content).to include(
        "add_index :crm_synchronisations, :crm_id, unique: true"
      )
      expect(content).to include(
        "add_index :crm_synchronisations, %i[resource_type resource_id], " \
        "unique: true, name: \"idx_crm_sync_on_resource\""
      )
      expect(content).to include(
        "add_index :crm_synchronisations, :last_synced_at"
      )
      expect(content).to include(
        "add_index :crm_synchronisations, :resource_type"
      )
      expect(content).to include(
        "add_index :crm_synchronisations, :resource_id"
      )
      # Composite index literal (note: template uses :crm_name here)
      expect(content).to include(
        "[:crm_name, :resource_type, :resource_id],"
      )
      expect(content).to include(
        "name: \"idx_unique_crm_sync_resource_crm\""
      )
    end
  end

  describe "private helpers" do
    it "file_name returns default when name is blank (\"\")",
       :aggregate_failures do
      gen = build_generator([""])
      expect(gen.send(:file_name)).to eq(
        described_class::DEFAULT_MIGRATION_FILENAME
      )
    end

    it "file_name underscores a provided CamelCase name",
       :aggregate_failures do
      gen = build_generator(["AddFooBar"])
      expect(gen.send(:file_name)).to eq("add_foo_bar")
    end
  end

  describe ".next_migration_number" do
    it "returns a UTC timestamp (YYYYMMDDHHMMSS) and is deterministic " \
       "for the same time", :aggregate_failures do
      fixed = Time.utc(2025, 8, 22, 9, 45, 12)
      allow(Time).to receive(:now).and_return(fixed)

      n1 = described_class.next_migration_number("ignored")
      n2 = described_class.next_migration_number("ignored")

      expect(n1).to match(/\A\d{14}\z/)
      expect(n1).to eq("20250822094512")
      expect(n2).to eq(n1)
    end
  end
end
