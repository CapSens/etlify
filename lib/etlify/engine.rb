require "rails/engine"

begin
  require "rake"
rescue LoadError
  # Rake is not available, skip silently.
end

module Etlify
  class Engine < ::Rails::Engine
    isolate_namespace Etlify

    # Columns that must exist on crm_synchronisations, with their fix command.
    REQUIRED_COLUMNS = {
      crm_name: "rails g migration AddCrmNameToCrmSynchronisations" \
                " crm_name:string:index && rails db:migrate",
      error_count: "rails g etlify:add_error_count && rails db:migrate",
    }.freeze

    initializer "etlify.check_schema" do
      # Defer until AR is loaded to avoid touching the connection too early.
      ActiveSupport.on_load(:active_record) do
        Etlify::Engine.check_schema_safely
      end
    end

    initializer "etlify.check_pending_syncs_table" do
      ActiveSupport.on_load(:active_record) do
        Etlify::Engine.check_pending_syncs_table_safely
      end
    end

    # --- Schema check ---------------------------------------------------------
    def self.check_schema_safely
      return if skip_schema_checks?

      begin
        connection = ActiveRecord::Base.connection
        table = "crm_synchronisations"

        unless connection.data_source_exists?(table)
          log_debug('Skip check: table "crm_synchronisations" does not exist.')
          return
        end

        REQUIRED_COLUMNS.each do |col, fix_command|
          if connection.column_exists?(table, col)
            log_debug("Column \"#{col}\" found on \"crm_synchronisations\".")
          else
            warn_missing_column(col, fix_command)
          end
        end
      rescue ActiveRecord::NoDatabaseError, ActiveRecord::StatementInvalid => e
        log_debug("Skip check: DB not ready (#{e.class}: #{e.message})")
      end
    end

    # --- Pending syncs table check -------------------------------------------
    def self.check_pending_syncs_table_safely
      return if skip_schema_checks?

      begin
        connection = ActiveRecord::Base.connection
        return if connection.data_source_exists?("etlify_pending_syncs")

        # Only warn if at least one model uses sync_dependencies.
        has_sync_deps = Etlify::Model.__included_klasses__.any? do |klass|
          next false unless klass.respond_to?(:etlify_crms) && klass.etlify_crms.present?

          klass.etlify_crms.values.any? { |conf| conf[:sync_dependencies]&.any? }
        end

        warn_missing_pending_syncs_table if has_sync_deps
      rescue ActiveRecord::NoDatabaseError, ActiveRecord::StatementInvalid => e
        log_debug("Skip check: DB not ready (#{e.class}: #{e.message})")
      end
    end

    # --- Helpers --------------------------------------------------------------
    def self.warn_missing_pending_syncs_table
      msg =
        'Missing table "etlify_pending_syncs". ' \
        "Please run: rails g etlify:migration create_etlify_pending_syncs && rails db:migrate"

      Rails.logger.warn("[Etlify] #{msg}") if defined?(Rails.logger)

      if defined?(ActiveSupport::Deprecation::DEFAULT)
        ActiveSupport::Deprecation::DEFAULT.warn("[Etlify] #{msg}")
      else
        warn("[Etlify] #{msg}")
      end
    end

    def self.warn_missing_column(column, fix_command)
      msg =
        "Missing column \"#{column}\" on table \"crm_synchronisations\". " \
        "Please run: #{fix_command}"

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
      db_tasks = [
        "db:create",
        "db:drop",
        "db:environment:set",
        "db:prepare",
        "db:migrate",
        "db:rollback",
        "db:schema:load",
        "db:structure:load",
        "db:setup",
        "db:reset",
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
