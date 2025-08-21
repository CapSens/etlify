require "rails/engine"
require "action_dispatch/railtie"

module Etlify
  class Engine < ::Rails::Engine
    isolate_namespace Etlify

    initializer "etlify.check_crm_name_column" do
      ActiveSupport.on_load(:active_record) do
        if defined?(CrmSynchronisation) &&
            CrmSynchronisation.table_exists? &&
            !CrmSynchronisation.column_names.include?("crm_name")
          raise(
            Etlify::Errors::MissingColumnError,
            <<~MSG.squish
              Missing column "crm_name" on table "crm_synchronisations".
              Please generate a migration with:

                rails g migration AddCrmNameToCrmSynchronisations \
                  crm_name:string:index

              Then run: rails db:migrate
            MSG
          )
        end
      rescue ActiveRecord::NoDatabaseError, ActiveRecord::StatementInvalid
        # Happens during `rails db:create` or before schema is loaded.
        # Silently ignore; check will run again once DB is ready.
      end
    end
  end
end
