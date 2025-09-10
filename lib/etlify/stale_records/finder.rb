module Etlify
  module StaleRecords
    class Finder
      class << self
        # Public: Build a Hash of { ModelClass => ActiveRecord::Relation (ids only) }
        # models - Optional array of model classes to restrict the search.
        # Returns a Hash.
        def call(models: nil)
          targets = models || etlified_models
          targets.each_with_object({}) do |model, h|
            next unless model.table_exists?

            h[model] = stale_relation_for(model)
          end
        end

        private

        # Detect models that actually called `etlified_with`.
        def etlified_models
          ActiveRecord::Base.descendants.select do |m|
            next false unless m.respond_to?(:table_exists?) && m.table_exists?

            m.respond_to?(:etlify_crm_object_type) &&
              m.etlify_crm_object_type.present?
          end
        end

        # Build the relation returning only PKs for stale/missing crm sync rows.
        def stale_relation_for(model)
          conn = model.connection
          epoch = epoch_literal(conn)

          threshold_sql = latest_timestamp_sql(model, epoch)

          crm_tbl = CrmSynchronisation.table_name
          crm_last_synced =
            "COALESCE(#{quoted(crm_tbl, 'last_synced_at', conn)}, #{epoch})"

          where_sql = <<-SQL.squish
            #{quoted(crm_tbl, 'id', conn)} IS NULL OR
            #{crm_last_synced} < (#{threshold_sql})
          SQL

          model
            .left_outer_joins(:crm_synchronisation)
            .where(Arel.sql(where_sql))
            .select(model.arel_table[model.primary_key])
        end

        # Build SQL for the "latest updated_at" across record and its deps.
        def latest_timestamp_sql(model, epoch)
          conn = model.connection
          owner_tbl = model.table_name

          parts = [
            "COALESCE(#{quoted(owner_tbl, 'updated_at', conn)}, #{epoch})",
          ]

          Array(model.try(:etlify_dependencies)).each do |dep_name|
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
          conn = model.connection
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
              preds << "#{quoted(dep_tbl, type_col, conn)} = " \
                       "#{conn.quote(model.name)}"
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

        # has_* :through => build a correlated subquery joining through->source.
        def through_dependency_timestamp_sql(model, reflection, epoch)
          conn       = model.connection
          through    = reflection.through_reflection
          source     = reflection.source_reflection

          through_tbl = through.klass.table_name
          source_tbl  = reflection.klass.table_name
          source_pk   = reflection.klass.primary_key
          owner_tbl   = model.table_name

          # Filter "through" rows that point to the owner.
          preds = []
          preds << "#{quoted(through_tbl, through.foreign_key, conn)} = " \
                   "#{quoted(owner_tbl, model.primary_key, conn)}"
          if (as = through.options[:as])
            preds << "#{quoted(through_tbl, "#{as}_type", conn)} = " \
                     "#{conn.quote(model.name)}"
          end

          # Join direction depends on source.macro:
          # - belongs_to  => through has FK to source
          # - has_one/many => source has FK to through
          join_on =
            case source.macro
            when :belongs_to
              # Example: memberships belongs_to :team
              # teams.id = memberships.team_id
              "#{quoted(source_tbl, source_pk, conn)} = " \
              "#{quoted(through_tbl, source.foreign_key, conn)}"
            when :has_one, :has_many
              # Example: profiles has_many :subscriptions
              # subscriptions.users_profile_id = users_profiles.id
              "#{quoted(source_tbl, source.foreign_key, conn)} = " \
              "#{quoted(through_tbl, through.klass.primary_key, conn)}"
            else
              # Unknown macro: return epoch guard to avoid SQL errors.
              return epoch
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

        # Polymorphic belongs_to: enumerate concrete types present in data.
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
                WHERE #{quoted(owner_tbl, type_col, conn)} =
                      #{conn.quote(type_name)}
                  AND #{quoted(dep_tbl, dep_pk, conn)} =
                      #{quoted(owner_tbl, fk, conn)}
                LIMIT 1
              ), #{epoch})
            SQL
          end

          return epoch if parts.empty?

          greatest(parts, conn)
        end

        # Adapter portability helpers.
        def greatest(parts, conn)
          # If there is only one expression, return it as-is to avoid SQLite
          # interpreting MAX(expr) as an aggregate in WHERE.
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
          "#{conn.quote_table_name(table)}." \
            "#{conn.quote_column_name(column)}"
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
