require "rails_helper"

RSpec.describe Etlify::Engine do
  def run_initializer
    # Re-run only the initializer we want, after our stubs.
    # Because ActiveRecord is already loaded, on_load(:active_record)
    # will execute the block immediately, using our stubs.
    initializer = Etlify::Engine.initializers.find do |i|
      i.name == "etlify.check_crm_name_column"
    end

    # Safety: make sure we actually found it.
    raise "Initializer not found" unless initializer

    initializer.run(Etlify::Engine.instance)
  end

  context "when the required column is present" do
    it "does not raise" do
      expect { run_initializer }.not_to raise_error
    end
  end

  context "when the required column is missing" do
    before do
      # Do not mutate schema: stub what the initializer reads
      allow(CrmSynchronisation).to receive(:table_exists?).and_return(true)
      allow(CrmSynchronisation).to receive(:column_names).and_return([
        "id",
        "crm_id",
        "last_digest",
        "last_synced_at",
        "last_error",
        "resource_type",
        "resource_id",
        "created_at",
        "updated_at",
      ])
      CrmSynchronisation.reset_column_information
    end

    it "raises a missing column error" do
      expect { run_initializer }.to raise_error(
        Etlify::Engine.missing_crm_name_warning_message
      )
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
