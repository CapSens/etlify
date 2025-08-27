require "rails/engine"
require "action_dispatch/railtie"

module Etlify
  class Engine < ::Rails::Engine
    isolate_namespace Etlify

    initializer "etlify.check_crm_name_column" do
      ActiveSupport.on_load(:active_record) do
        # Skip check during DB tasks or if explicitly disabled.
        next if Etlify.db_task_running? || ENV["SKIP_ETLIFY_DB_CHECK"] == "1"

        begin
          # Ensure model and table exist before checking the column.
          next unless defined?(CrmSynchronisation)

          conn = ActiveRecord::Base.connection
          table = "crm_synchronisations"

          next unless conn.data_source_exists?(table)

          has_column = conn.column_exists?(table, "crm_name")
          next if has_column

          raise(
            Etlify::MissingColumnError,
            <<~MSG.squish
              Missing column "crm_name" on table "crm_synchronisations".
              Please generate a migration with:

                rails g migration AddCrmNameToCrmSynchronisations \
                  crm_name:string:index

              Then run: rails db:migrate
            MSG
          )
        rescue ActiveRecord::NoDatabaseError, ActiveRecord::StatementInvalid
          # Happens during `db:create`, before schema is loaded, etc.
          # Silently ignore; check will run again once DB is ready.
        end
      end
    end
  end

  # Detect if a database-related rake task is running.
  def self.db_task_running?
    # Rake may not be loaded outside tasks.
    return false unless defined?(Rake)

    tasks = Rake.application.top_level_tasks
    tasks.any? do |t|
      t.start_with?("db:") || t.start_with?("app:db:")
    end
  rescue
    # Be conservative: if unsure, do not block boot.
    false
  end
end
