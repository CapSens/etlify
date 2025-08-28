# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::Engine do
  def run_initializer
    # Triggers the on_load(:active_record) hooks
    ActiveSupport.run_load_hooks(:active_record, ActiveRecord::Base)
  end

  context "when the required column is present" do
    it "does not raise" do
      expect { run_initializer }.not_to raise_error
    end
  end

  context "when the required column is missing (logging mode)" do
    before do
      # Do not mutate schema: stub what the initializer reads
      allow(CrmSynchronisation).to receive(:table_exists?).and_return(true)
      allow(CrmSynchronisation).to receive(:column_names).and_return(
        %w[
          id crm_id last_digest last_synced_at last_error
          resource_type resource_id created_at updated_at
        ]
      )
      CrmSynchronisation.reset_column_information
    end

    it "logs a helpful error" do
      # Use a test logger spy without printing to STDOUT/STDERR
      test_logger = instance_double(Logger)
      allow(test_logger).to receive(:error)
      allow(Etlify.config).to receive(:logger).and_return(test_logger)

      # Ensure the initializer is registered, then trigger the hook
      init = Etlify::Engine.initializers.find do |i|
        i.name == "etlify.check_crm_name_column"
      end
      init.run(Etlify::Engine.instance)

      run_initializer

      expect(test_logger).to have_received(:error).with(
        match(/Missing column "crm_name" on table "crm_synchronisations"/)
      ).at_least(:once)
    end
  end

  context "when DB is not ready yet" do
    it "ignores ActiveRecord::NoDatabaseError" do
      allow(ActiveRecord::Base).to receive(:connection)
        .and_raise(ActiveRecord::NoDatabaseError)

      expect { ActiveSupport.run_load_hooks(:active_record, nil) }
        .not_to raise_error
    end

    it "ignores ActiveRecord::StatementInvalid" do
      allow(CrmSynchronisation).to receive(:table_exists?)
        .and_raise(ActiveRecord::StatementInvalid.new("boom"))

      expect { run_initializer }.not_to raise_error
    end
  end
end
