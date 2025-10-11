require "rails/generators"
require "rails/generators/active_record"

module Etlify
  module Generators
    class MigrationGenerator < ActiveRecord::Generators::Base
      DEFAULT_MIGRATION_FILENAME = "create_crm_synchronisations"

      source_root File.expand_path("templates", __dir__)

      def copy_migration
        migration_template(
          "create_crm_synchronisations.rb.tt",
          "db/migrate/#{file_name}.rb"
        )
      end

      private

      def file_name
        (name.presence || DEFAULT_MIGRATION_FILENAME).underscore
      end

      def self.next_migration_number(_dirname)
        Time.now.utc.strftime("%Y%m%d%H%M%S")
      end
    end
  end
end
