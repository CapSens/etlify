require "rails/engine"

begin
  require "rake"
rescue LoadError
  # Rake is not available, skip silently.
end

module Etlify
  class Engine < ::Rails::Engine
    isolate_namespace Etlify

    initializer "etlify.check_crm_name_column" do
      # Defer until AR is loaded to avoid touching the connection too early.
      ActiveSupport.on_load(:active_record) do
        Etlify::Engine.check_crm_name_column_safely
      end
    end

    # --- Schema check ---------------------------------------------------------
    def self.check_crm_name_column_safely
      return if skip_schema_checks?

      begin
        connection = ActiveRecord::Base.connection
        table = "crm_synchronisations"
        col = :crm_name

        unless connection.data_source_exists?(table)
          log_debug('Skip check: table "crm_synchronisations" does not exist.')
          return
        end

        if connection.column_exists?(table, col)
          log_debug('Column "crm_name" found on "crm_synchronisations".')
          return
        end

        warn_missing_column
      rescue ActiveRecord::NoDatabaseError, ActiveRecord::StatementInvalid => e
        log_debug("Skip check: DB not ready (#{e.class}: #{e.message})")
      end
    end

    # --- Helpers --------------------------------------------------------------
    def self.warn_missing_column
      msg =
        'Missing column "crm_name" on table "crm_synchronisations". ' \
        "Please run: rails g migration " \
        "AddCrmNameToCrmSynchronisations crm_name:string:index && rails db:migrate"

      Rails.logger.warn("[Etlify] #{msg}") if defined?(Rails.logger)

      # Use instance, not class method
      if defined?(ActiveSupport::Deprecation::DEFAULT)
        ActiveSupport::Deprecation::DEFAULT.warn("[Etlify] #{msg}")
      else
        warn("[Etlify] #{msg}") # Fallback to kernel warn if deprecation missing
      end
    end

    def self.log_debug(msg)
      return unless defined?(Rails.logger)

      Rails.logger.debug("[Etlify] #{msg}")
    end

    def self.skip_schema_checks?
      # Allow explicit opt-out via env var.
      return true if ENV["ETLIFY_SKIP_SCHEMA_CHECK"] == "1"

      # Skip when running rake/rails DB tasks or assets precompile.
      return true if running_db_task?
      return true if assets_precompile?

      # Skip in rails console to avoid noisy logs.
      return true if rails_console?

      false
    end

    def self.running_db_task?
      # If rake not loaded or application not available, assume no db task.
      return false unless defined?(Rake)
      return false unless Rake.respond_to?(:application)
      return false unless (app = Rake.application)

      tasks = begin
        app.top_level_tasks
      rescue
        []
      end
      db_tasks = %w[
        db:create db:drop db:environment:set db:prepare db:migrate db:rollback
        db:schema:load db:structure:load db:setup db:reset
      ]
      (tasks & db_tasks).any?
    end

    def self.assets_precompile?
      # Common CI/CD detection first.
      return true if ENV["RAILS_GROUPS"]&.include?("assets")
      return true if ENV["ASSETS_PRECOMPILE"] == "1"

      return false unless defined?(Rake)
      return false unless Rake.respond_to?(:application)
      return false unless (app = Rake.application)

      tasks = begin
        app.top_level_tasks
      rescue
        []
      end
      tasks.include?("assets:precompile")
    end

    def self.rails_console?
      defined?(Rails::Console)
    end
  end
end
