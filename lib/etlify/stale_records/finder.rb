module Etlify
  module StaleRecords
    class Finder
      class << self
        # Public: Build a nested Hash of
        #   { ModelClass => { crm_sym => ActiveRecord::Relation(ids only) } }
        # models   - Optional Array of model classes to restrict the search.
        # crm_name - Optional Symbol/String to target a single CRM.
        # Returns a Hash.
        def call(models: nil, crm_name: nil)
          targets = models || etlified_models(crm_name: crm_name)
          targets.each_with_object({}) do |model, out|
            next unless model.table_exists?

            crms = configured_crm_names_for(model, crm_name: crm_name)
            next if crms.empty?

            out[model] = crms.each_with_object({}) do |crm, per_crm|
              per_crm[crm] = stale_relation_for(model, crm_name: crm)
            end
          end
        end

        private

        # Detect models that included Etlify::Model and have at least one CRM
        # configured (optionally filtered by crm_name).
        def etlified_models(crm_name: nil)
          ActiveRecord::Base.descendants.select do |m|
            next false unless m.respond_to?(:table_exists?) && m.table_exists?
            next false unless m.respond_to?(:etlify_crms) && m.etlify_crms.present?

            if crm_name
              m.etlify_crms.key?(crm_name.to_sym)
            else
              m.etlify_crms.any?
            end
          end
        end

        # List configured CRM names for a model (optionally filtered).
        def configured_crm_names_for(model, crm_name: nil)
          return [] unless model.respond_to?(:etlify_crms) && model.etlify_crms.present?
          return [crm_name.to_sym] if crm_name && model.etlify_crms.key?(crm_name.to_sym)

          model.etlify_crms.keys
        end

        # Build the relation returning only PKs for stale/missing sync rows
        # for the given CRM. The JOIN is scoped by crm_name.
        def stale_relation_for(model, crm_name:)
          conn      = model.connection
          owner_tbl = model.table_name
          owner_pk  = model.primary_key
          crm_tbl   = CrmSynchronisation.table_name
          epoch     = epoch_literal(conn)

          threshold_sql = latest_timestamp_sql(model, epoch, crm_name: crm_name)

          # Scope the LEFT OUTER JOIN to the specific crm_name and owner row.
          join_on = [
            "#{quoted(crm_tbl, 'resource_type', conn)} = #{conn.quote(model.name)}",
            "#{quoted(crm_tbl, 'resource_id', conn)} = " \
              "#{quoted(owner_tbl, owner_pk, conn)}",
            "#{quoted(crm_tbl, 'crm_name', conn)} = #{conn.quote(crm_name.to_s)}",
          ].join(" AND ")

          last_synced = "COALESCE(#{quoted(crm_tbl, 'last_synced_at', conn)}, #{epoch})"

          where_sql = <<-SQL.squish
            #{quoted(crm_tbl, 'id', conn)} IS NULL
            OR #{last_synced} < (#{threshold_sql})
          SQL

          model
            .joins("LEFT OUTER JOIN #{conn.quote_table_name(crm_tbl)} ON #{join_on}")
            .where(Arel.sql(where_sql))
            .select(model.arel_table[owner_pk])
        end

        # Build SQL for "latest updated_at" across record and its CRM-specific deps.
        def latest_timestamp_sql(model, epoch, crm_name:)
          conn      = model.connection
          owner_tbl = model.table_name

          parts = ["COALESCE(#{quoted(owner_tbl, 'updated_at', conn)}, #{epoch})"]

          deps = Array(
            model.etlify_crms.dig(crm_name.to_sym, :dependencies)
          ).map(&:to_sym)

          deps.each do |dep_name|
            reflection = model.reflect_on_association(dep_name)
            next unless reflection

            parts << dependency_max_timestamp_sql(model, reflection, epoch)
          end

          greatest(parts, conn)
        end

        # Route to the proper builder depending on the association shape.
        def dependency_max_timestamp_sql(model, reflection, epoch)
          if reflection.polymorphic? && reflection.macro == :belongs_to
            polymorphic_belongs_to_timestamp_sql(model, reflection, epoch)
          elsif reflection.through_reflection
            through_dependency_timestamp_sql(model, reflection, epoch)
          else
            direct_dependency_timestamp_sql(model, reflection, epoch)
          end
        end

        # Non-through associations.
        def direct_dependency_timestamp_sql(model, reflection, epoch)
          conn      = model.connection
          owner_tbl = model.table_name

          case reflection.macro
          when :belongs_to
            dep_tbl = reflection.klass.table_name
            dep_pk  = reflection.klass.primary_key
            fk      = reflection.foreign_key

            sub = <<-SQL.squish
              SELECT #{quoted(dep_tbl, 'updated_at', conn)}
              FROM #{conn.quote_table_name(dep_tbl)}
              WHERE #{quoted(dep_tbl, dep_pk, conn)} =
                    #{quoted(owner_tbl, fk, conn)}
              LIMIT 1
            SQL
            "COALESCE((#{sub}), #{epoch})"
          when :has_one, :has_many
            dep_tbl = reflection.klass.table_name
            fk      = reflection.foreign_key

            preds = []
            preds << "#{quoted(dep_tbl, fk, conn)} = " \
                     "#{quoted(owner_tbl, model.primary_key, conn)}"

            if (poly_as = reflection.options[:as])
              type_col = "#{poly_as}_type"
              preds << "#{quoted(dep_tbl, type_col, conn)} = #{conn.quote(model.name)}"
            end

            sub = <<-SQL.squish
              SELECT MAX(#{quoted(dep_tbl, 'updated_at', conn)})
              FROM #{conn.quote_table_name(dep_tbl)}
              WHERE #{preds.map { |p| "(#{p})" }.join(' AND ')}
            SQL
            "COALESCE((#{sub}), #{epoch})"
          else
            # Unknown macro: safely ignore with epoch fallback.
            epoch
          end
        end

        # has_* :through => correlated subquery from through -> source.
        def through_dependency_timestamp_sql(model, reflection, epoch)
          conn       = model.connection
          through    = reflection.through_reflection
          source     = reflection.source_reflection

          through_tbl = through.klass.table_name
          source_tbl  = reflection.klass.table_name
          source_pk   = reflection.klass.primary_key
          owner_tbl   = model.table_name

          preds = []
          preds << "#{quoted(through_tbl, through.foreign_key, conn)} = " \
                  "#{quoted(owner_tbl, model.primary_key, conn)}"
          if (as = through.options[:as])
            preds << "#{quoted(through_tbl, "#{as}_type", conn)} = " \
                    "#{conn.quote(model.name)}"
          end

          # 🔧 Join orientation fix:
          # If source is belongs_to, FK lives on the through table.
          # Else (has_many/has_one), FK lives on the source table.
          join_on =
            if source.macro == :belongs_to
              "#{quoted(source_tbl, source_pk, conn)} = " \
              "#{quoted(through_tbl, source.foreign_key, conn)}"
            else
              "#{quoted(source_tbl, source.foreign_key, conn)} = " \
              "#{quoted(through_tbl, through.klass.primary_key, conn)}"
            end

          sub = <<-SQL.squish
            SELECT MAX(#{quoted(source_tbl, 'updated_at', conn)})
            FROM #{conn.quote_table_name(through_tbl)}
            INNER JOIN #{conn.quote_table_name(source_tbl)}
                    ON #{join_on}
            WHERE #{preds.map { |p| "(#{p})" }.join(' AND ')}
          SQL

          "COALESCE((#{sub}), #{epoch})"
        end

        # belongs_to polymorphic: enumerate concrete types and pick greatest ts.
        def polymorphic_belongs_to_timestamp_sql(model, reflection, epoch)
          conn      = model.connection
          owner_tbl = model.table_name
          fk        = reflection.foreign_key
          type_col  = reflection.foreign_type

          types = model.distinct.pluck(type_col).compact.uniq

          parts = types.filter_map do |type_name|
            klass = safe_constantize(type_name)
            next nil unless klass&.respond_to?(:table_name)

            dep_tbl = klass.table_name
            dep_pk  = klass.primary_key

            <<-SQL.squish
              COALESCE((
                SELECT #{quoted(dep_tbl, 'updated_at', conn)}
                FROM #{conn.quote_table_name(dep_tbl)}
                WHERE #{quoted(owner_tbl, type_col, conn)} = #{conn.quote(type_name)}
                  AND #{quoted(dep_tbl, dep_pk, conn)} = #{quoted(owner_tbl, fk, conn)}
                LIMIT 1
              ), #{epoch})
            SQL
          end

          return epoch if parts.empty?

          greatest(parts, conn)
        end

        # Adapter portability helpers.
        def greatest(parts, conn)
          # With a single expression, return as-is (SQLite aggregate quirk).
          return parts.first if parts.size == 1

          fn = greatest_function_name(conn)
          "#{fn}(#{parts.join(', ')})"
        end

        def greatest_function_name(conn)
          adapter = conn.adapter_name.to_s.downcase
          adapter.include?("postgres") ? "GREATEST" : "MAX"
        end

        def epoch_literal(conn)
          adapter = conn.adapter_name.to_s.downcase
          if adapter.include?("postgres")
            "TIMESTAMP '1970-01-01 00:00:00'"
          else
            "DATETIME('1970-01-01 00:00:00')"
          end
        end

        def quoted(table, column, conn)
          "#{conn.quote_table_name(table)}.#{conn.quote_column_name(column)}"
        end

        def safe_constantize(str)
          str.constantize
        rescue NameError
          nil
        end
      end
    end
  end
end
