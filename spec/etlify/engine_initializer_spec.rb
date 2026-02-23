require "rails_helper"

RSpec.describe Etlify::Engine do
  def run_initializer
    initializer = Etlify::Engine.initializers.find do |i|
      i.name == "etlify.check_schema"
    end
    raise "Initializer not found" unless initializer

    initializer.run(Etlify::Engine.instance)
  end

  # Minimal AR connection API stub for the initializer.
  def stub_connection(data_source_exists:, columns_present: [])
    conn = instance_double("ActiveRecord::ConnectionAdapters::AbstractAdapter")

    allow(conn).to receive(:data_source_exists?)
      .with("crm_synchronisations").and_return(data_source_exists)

    allow(conn).to receive(:column_exists?) do |_table, col|
      columns_present.include?(col)
    end

    allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
    conn
  end

  before do
    # Ensure the initializer is not short-circuited.
    allow(Etlify::Engine).to receive(:running_db_task?).and_return(false)

    # Default: let debug logs be no-op and accept any args.
    allow(Etlify::Engine).to receive(:log_debug).with(*anything)
  end

  context "when all required columns are present" do
    it "does not warn and does not raise" do
      stub_connection(
        data_source_exists: true,
        columns_present: Etlify::Engine::REQUIRED_COLUMNS.keys
      )

      expect(Etlify::Engine).not_to receive(:warn_missing_column)

      expect { run_initializer }.not_to raise_error
    end
  end

  context "when a required column is missing" do
    it "emits a warning for each missing column" do
      stub_connection(
        data_source_exists: true,
        columns_present: [:crm_name]
      )

      expect(Etlify::Engine).to receive(:warn_missing_column)
        .with(:error_count, anything)

      expect { run_initializer }.not_to raise_error
    end
  end

  context "when the table does not exist yet" do
    it "does not warn and does not raise" do
      stub_connection(data_source_exists: false)

      expect(Etlify::Engine).not_to receive(:warn_missing_column)

      expect { run_initializer }.not_to raise_error
    end
  end

  context "when DB is not ready yet" do
    it "ignores ActiveRecord::NoDatabaseError" do
      allow(ActiveRecord::Base).to receive(:connection)
        .and_raise(ActiveRecord::NoDatabaseError)

      expect { run_initializer }.not_to raise_error
    end

    it "ignores ActiveRecord::StatementInvalid" do
      conn = instance_double(
        "ActiveRecord::ConnectionAdapters::AbstractAdapter"
      )
      allow(conn).to receive(:data_source_exists?)
        .and_raise(ActiveRecord::StatementInvalid.new("boom"))
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)

      expect { run_initializer }.not_to raise_error
    end
  end
end

RSpec.describe Etlify::Engine, "pending_syncs table check" do
  def run_pending_syncs_initializer
    initializer = Etlify::Engine.initializers.find do |i|
      i.name == "etlify.check_pending_syncs_table"
    end
    raise "Initializer not found" unless initializer

    initializer.run(Etlify::Engine.instance)
  end

  before do
    allow(Etlify::Engine).to receive(:running_db_task?).and_return(false)
    allow(Etlify::Engine).to receive(:log_debug).with(*anything)
  end

  context "when the table exists" do
    it "does not warn" do
      conn = instance_double("ActiveRecord::ConnectionAdapters::AbstractAdapter")
      allow(conn).to receive(:data_source_exists?)
        .with("etlify_pending_syncs").and_return(true)
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)

      expect(Etlify::Engine).not_to receive(:warn_missing_pending_syncs_table)
      expect { run_pending_syncs_initializer }.not_to raise_error
    end
  end

  context "when the table is missing and a model uses sync_dependencies" do
    it "emits a warning" do
      conn = instance_double("ActiveRecord::ConnectionAdapters::AbstractAdapter")
      allow(conn).to receive(:data_source_exists?)
        .with("etlify_pending_syncs").and_return(false)
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)

      # Simulate a model with sync_dependencies
      klass = double("ModelClass",
        respond_to?: true,
        etlify_crms: {hubspot: {sync_dependencies: [:company]}}
      )
      allow(klass).to receive(:respond_to?).with(:etlify_crms).and_return(true)
      allow(Etlify::Model).to receive(:__included_klasses__).and_return([klass])

      expect(Etlify::Engine).to receive(:warn_missing_pending_syncs_table)
      expect { run_pending_syncs_initializer }.not_to raise_error
    end
  end

  context "when no model uses sync_dependencies" do
    it "does not warn even if table is missing" do
      conn = instance_double("ActiveRecord::ConnectionAdapters::AbstractAdapter")
      allow(conn).to receive(:data_source_exists?)
        .with("etlify_pending_syncs").and_return(false)
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)

      klass = double("ModelClass",
        respond_to?: true,
        etlify_crms: {hubspot: {sync_dependencies: []}}
      )
      allow(klass).to receive(:respond_to?).with(:etlify_crms).and_return(true)
      allow(Etlify::Model).to receive(:__included_klasses__).and_return([klass])

      expect(Etlify::Engine).not_to receive(:warn_missing_pending_syncs_table)
      expect { run_pending_syncs_initializer }.not_to raise_error
    end
  end
end
