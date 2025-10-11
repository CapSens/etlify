require "rails_helper"

RSpec.describe Etlify::Engine do
  def run_initializer
    initializer = Etlify::Engine.initializers.find do |i|
      i.name == "etlify.check_crm_name_column"
    end
    raise "Initializer not found" unless initializer

    initializer.run(Etlify::Engine.instance)
  end

  # Minimal AR connection API stub for the initializer.
  def stub_connection(data_source_exists:, column_exists:)
    conn = instance_double("ActiveRecord::ConnectionAdapters::AbstractAdapter")

    allow(conn).to receive(:data_source_exists?)
      .with("crm_synchronisations").and_return(data_source_exists)

    allow(conn).to receive(:column_exists?)
      .with("crm_synchronisations", :crm_name).and_return(column_exists)

    allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
    conn
  end

  before do
    # Ensure the initializer is not short-circuited.
    allow(Etlify::Engine).to receive(:running_db_task?).and_return(false)

    # Default: let debug logs be no-op and accept any args.
    allow(Etlify::Engine).to receive(:log_debug).with(*anything)
  end

  context "when the required column is present" do
    it "does not warn and does not raise" do
      stub_connection(data_source_exists: true, column_exists: true)

      # Only assert that no warning helper is called.
      expect(Etlify::Engine).not_to receive(:warn_missing_column)

      expect { run_initializer }.not_to raise_error
    end
  end

  context "when the required column is missing" do
    it "emits a warning and does not raise" do
      stub_connection(data_source_exists: true, column_exists: false)

      # Engine currently calls warn helper with no args.
      expect(Etlify::Engine).to receive(:warn_missing_column).with(no_args)

      expect { run_initializer }.not_to raise_error
    end
  end

  context "when the table does not exist yet" do
    it "does not warn and does not raise" do
      stub_connection(data_source_exists: false, column_exists: false)

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
