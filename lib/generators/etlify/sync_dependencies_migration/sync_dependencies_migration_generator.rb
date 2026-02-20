# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Etlify
  module Generators
    class SyncDependenciesMigrationGenerator < ActiveRecord::Generators::Base
      DEFAULT_MIGRATION_FILENAME = "create_etlify_sync_dependencies"

      source_root File.expand_path("templates", __dir__)

      def copy_migration
        migration_template(
          "create_etlify_sync_dependencies.rb.tt",
          "db/migrate/#{file_name}.rb"
        )
      end

      def self.next_migration_number(_dirname)
        Time.now.utc.strftime("%Y%m%d%H%M%S")
      end

      private

      def file_name
        (name.presence || DEFAULT_MIGRATION_FILENAME).underscore
      end
    end
  end
end
