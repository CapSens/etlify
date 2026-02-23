require "rails/generators"
require "rails/generators/active_record"

module Etlify
  module Generators
    class MigrationGenerator < ActiveRecord::Generators::Base
      TEMPLATES = {
        "create_crm_synchronisations" => "create_crm_synchronisations.rb.tt",
        "create_etlify_pending_syncs" => "create_etlify_pending_syncs.rb.tt",
      }.freeze

      source_root File.expand_path("templates", __dir__)

      def copy_migration
        template_name = TEMPLATES.fetch(file_name, TEMPLATES.values.first)

        migration_template(
          template_name,
          "db/migrate/#{file_name}.rb"
        )
      end

      private

      def file_name
        (name.presence || TEMPLATES.keys.first).underscore
      end

      def self.next_migration_number(_dirname)
        Time.now.utc.strftime("%Y%m%d%H%M%S")
      end
    end
  end
end
