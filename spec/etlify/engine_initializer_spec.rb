# frozen_string_literal: true

require "rails_helper"

RSpec.describe Etlify::Engine do
  def run_initializer
    ActiveSupport.run_load_hooks(:active_record, ActiveRecord::Base)
  end

  context "when the required column is present" do
    it "does not raise" do
      # L'initializer a déjà été exécuté au chargement de la gem,
      # son on_load est donc enregistré : on peut juste déclencher le hook.
      expect { run_initializer }.not_to raise_error
    end
  end

  context "when the required column is missing" do
    before do
      # Drop + recreate the table without the crm_name column.
      ActiveRecord::Base.connection.execute(
        "DROP TABLE IF EXISTS crm_synchronisations"
      )
      ActiveRecord::Schema.define do
        create_table :crm_synchronisations, force: true do |t|
          # crm_name intentionally omitted
          t.string   :crm_id
          t.string   :last_digest
          t.datetime :last_synced_at
          t.text     :last_error
          t.string   :resource_type, null: false
          t.integer  :resource_id, null: false
          t.timestamps
        end
      end
      CrmSynchronisation.reset_column_information
    end

    after do
      # Restore correct schema for other specs.
      ActiveRecord::Base.connection.execute(
        "DROP TABLE IF EXISTS crm_synchronisations"
      )
      ActiveRecord::Schema.define do
        create_table :crm_synchronisations, force: true do |t|
          t.string  :crm_name, null: false
          t.string  :crm_id
          t.string  :last_digest
          t.datetime :last_synced_at
          t.text    :last_error
          t.string  :resource_type, null: false
          t.integer :resource_id, null: false
          t.timestamps
        end
        add_index :crm_synchronisations,
                  %i[resource_type resource_id crm_name],
                  unique: true,
                  name: "idx_sync_polymorphic_unique"
      end
      CrmSynchronisation.reset_column_information
    end

    it "raises a helpful Etlify::MissingColumnError" do
      # Find the initializer and run it INSIDE the expectation.
      init = Etlify::Engine.initializers.find do |i|
        i.name == "etlify.check_crm_name_column"
      end

      expect { init.run(Etlify::Engine.instance) }.to raise_error(
        Etlify::MissingColumnError,
        /Missing column "crm_name" on table "crm_synchronisations"/
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
