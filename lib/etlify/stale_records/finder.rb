module Etlify
  module StaleRecords
    # Finder builds, for each configured model/CRM, an ids-only relation
    # containing records that are considered "stale" and need to be synced.
    #
    # Return shape:
    #   {
    #     User    => { hubspot: <ActiveRecord::Relation> },
    #     Company => { hubspot: <ActiveRecord::Relation> }
    #   }
    #
    # Each returned relation:
    #   - selects only a single column "id"
    #   - is safe to `pluck(:id)` (no ambiguous columns)
    #   - is ordered ASC by id (stable batching)
    class Finder
      class << self
        # Public: Build a nested Hash of:
        #   { ModelClass => { crm_sym => ActiveRecord::Relation(ids only) } }
        #
        # models   - Optional Array of model classes to restrict the search.
        # crm_name - Optional Symbol/String to target a single CRM.
        #
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

        # ---------- Model discovery / filtering ----------

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

        def configured_crm_names_for(model, crm_name: nil)
          return [] unless model.respond_to?(:etlify_crms) && model.etlify_crms

          if crm_name && model.etlify_crms.key?(crm_name.to_sym)
            [crm_name.to_sym]
          else
            model.etlify_crms.keys
          end
        end

        # ---------- Core relation builder (Arel-based) ----------

        # Build the "stale" ids-only relation for one model/CRM.
        #
        # Strategy:
        #   - LEFT OUTER JOIN crm_synchronisations scoped to given CRM.
        #   - WHERE (crm_sync.id IS NULL OR crm_sync.last_synced_at < threshold)
        #   - threshold = GREATEST(owner.updated_at, deps.updated_at..., epoch)
        #   - SELECT "<owners>.<pk> AS id"
        #   - ORDER BY id
        #   - Wrap in a subquery exposing a single "id" column
        def stale_relation_for(model, crm_name:)
          conn       = model.connection
          owner_tbl  = model.table_name
          owner_arel = arel_table(model)
          crm_arel   = CrmSynchronisation.arel_table

          join_on =
            crm_arel[:resource_type].eq(model.name)
              .and(crm_arel[:resource_id].eq(owner_arel[model.primary_key]))
              .and(crm_arel[:crm_name].eq(crm_name.to_s))

          join_sql = owner_arel.create_join(
            crm_arel, owner_arel.create_on(join_on), Arel::Nodes::OuterJoin
          )

          threshold_expr =
            latest_timestamp_arel(model, crm_name: crm_name, conn: conn)

          last_synced_expr =
            Arel::Nodes::NamedFunction.new(
              "COALESCE", [crm_arel[:last_synced_at], epoch_arel(conn)]
            )

          where_pred =
            crm_arel[:id].eq(nil).or(last_synced_expr.lt(threshold_expr))

          qualified_pk_sql =
            "#{conn.quote_table_name(owner_tbl)}." \
            "#{conn.quote_column_name(model.primary_key)}"

          base_rel =
            model.unscoped
                 .from(owner_arel)
                 .joins(join_sql)
                 .where(where_pred)
                 .select(Arel.sql("#{qualified_pk_sql} AS id"))
                 .reorder(Arel.sql("#{qualified_pk_sql} ASC"))

          sub_sql  = base_rel.to_sql
          sub_from = Arel.sql("(#{sub_sql}) AS etlify_stale_ids")

          model.unscoped
               .from(sub_from)
               .select(Arel.sql("id"))
               .reorder(Arel.sql("id ASC"))
        end

        # ---------- Threshold (owner + dependencies) ----------

        # Build an Arel expression representing the newest timestamp among:
        #   - owner's timestamp column (updated_at or created_at)
        #   - each configured dependency's newest timestamp
        #   - epoch fallback when values are NULL or missing
        def latest_timestamp_arel(model, crm_name:, conn:)
          owner_arel = arel_table(model)
          parts = []

          # Owner timestamp (prefer updated_at, fallback created_at, else epoch)
          owner_ts_col = timestamp_column_for_model(model)
          owner_ts =
            if owner_ts_col
              Arel::Nodes::NamedFunction.new(
                fn_coalesce(conn),
                [owner_arel[owner_ts_col], epoch_arel(conn)]
              )
            else
              epoch_arel(conn)
            end
          parts << owner_ts

          deps =
            Array(
              model.etlify_crms.dig(crm_name.to_sym, :dependencies)
            ).map(&:to_sym)

          deps.each do |dep_name|
            reflection = model.reflect_on_association(dep_name)
            next unless reflection

            parts << dependency_max_timestamp_arel(model, reflection, conn)
          end

          greatest_arel(conn, *parts)
        end

        # Choose dependency strategy.
        def dependency_max_timestamp_arel(model, reflection, conn)
          if reflection.polymorphic? && reflection.macro == :belongs_to
            # Safer fallback: do not scan the table for type discovery.
            return epoch_arel(conn)
          end

          if reflection.through_reflection
            return Arel.sql(
              through_dependency_timestamp_sql(model, reflection, conn)
            )
          end

          if reflection.macro == :has_and_belongs_to_many
            return Arel.sql(
              habtm_dependency_timestamp_sql(model, reflection, conn)
            )
          end

          direct_dependency_timestamp_arel(model, reflection, conn)
        end

        # ---------- Direct associations (belongs_to / has_one / has_many) ---

        def direct_dependency_timestamp_arel(model, reflection, conn)
          owner_arel = arel_table(model)
          ts_col = dep_timestamp_column(reflection.klass)

          case reflection.macro
          when :belongs_to
            return epoch_arel(conn) unless ts_col

            dep_arel = reflection.klass.arel_table

            # Respect custom primary_key on the target.
            dep_pk =
              reflection.options[:primary_key] ||
              reflection.klass.primary_key

            fk = reflection.foreign_key

            sub =
              dep_arel
                .project(dep_arel[ts_col])
                .where(dep_arel[dep_pk].eq(owner_arel[fk]))
                .take(1)

            Arel::Nodes::NamedFunction.new(
              fn_coalesce(conn),
              [Arel::Nodes::Grouping.new(sub), epoch_arel(conn)]
            )

          when :has_one, :has_many
            return epoch_arel(conn) unless ts_col

            dep_arel = reflection.klass.arel_table

            # Use foreign_key on dependency pointing to owner primary key.
            fk = reflection.foreign_key

            preds = [dep_arel[fk].eq(owner_arel[model.primary_key])]

            # Respect polymorphic :as on the dependency if present.
            if (poly_as = reflection.options[:as])
              preds << dep_arel["#{poly_as}_type"].eq(model.name)
            end

            sub =
              dep_arel
                .project(
                  Arel::Nodes::NamedFunction.new("MAX", [dep_arel[ts_col]])
                )
                .where(preds.reduce(&:and))

            Arel::Nodes::NamedFunction.new(
              fn_coalesce(conn),
              [Arel::Nodes::Grouping.new(sub), epoch_arel(conn)]
            )

          else
            epoch_arel(conn)
          end
        end

        # ----------------------------- HABTM -------------------------------

        # Build MAX(timestamp) over the HABTM source table joined via the
        # join table. Respect custom foreign keys when available.
        def habtm_dependency_timestamp_sql(model, reflection, conn)
          owner_tbl  = model.table_name
          source_tbl = reflection.klass.table_name
          ts_col     = dep_timestamp_column(reflection.klass)
          return epoch_literal(conn) unless ts_col

          source_pk  =
            reflection.options[:association_primary_key] ||
            reflection.klass.primary_key

          source_qt  = conn.quote_table_name(source_tbl)
          source_tc  = conn.quote_column_name(ts_col)

          join_tbl   = reflection.join_table.to_s
          jt_qt      = conn.quote_table_name(join_tbl)

          owner_fk   = reflection.foreign_key.to_s
          source_fk  = reflection.association_foreign_key.to_s

          preds = []
          preds << "#{jt_qt}.#{conn.quote_column_name(owner_fk)} = " \
                   "#{conn.quote_table_name(owner_tbl)}." \
                   "#{conn.quote_column_name(model.primary_key)}"
          preds_sql = preds.map { |p| "(#{p})" }.join(" AND ")

          <<-SQL.squish
            COALESCE((
              SELECT MAX(#{source_qt}.#{source_tc})
              FROM #{jt_qt}
              INNER JOIN #{source_qt}
                ON #{source_qt}.#{conn.quote_column_name(source_pk)} =
                   #{jt_qt}.#{conn.quote_column_name(source_fk)}
              WHERE #{preds_sql}
            ), #{epoch_literal(conn)})
          SQL
        end

        # -------------------------- has_* :through -------------------------

        # MAX(timestamp) over the source table joined via the through table.
        # Tries to respect custom keys when present.
        def through_dependency_timestamp_sql(model, reflection, conn)
          through    = reflection.through_reflection
          source     = reflection.source_reflection

          ts_col = dep_timestamp_column(reflection.klass)
          return epoch_literal(conn) unless ts_col

          through_tbl = through.klass.table_name
          through_pk  =
            through.options[:primary_key] || through.klass.primary_key

          source_tbl  = reflection.klass.table_name
          source_pk   =
            source.options[:primary_key] || reflection.klass.primary_key

          owner_tbl   = model.table_name

          qt = ->(t) { conn.quote_table_name(t) }
          qc = ->(c) { conn.quote_column_name(c) }

          # Owner -> through join predicate
          owner_fk = through.foreign_key
          preds = []
          preds << "#{qt.call(through_tbl)}.#{qc.call(owner_fk)} = " \
                   "#{qt.call(owner_tbl)}.#{qc.call(model.primary_key)}"

          # Polymorphic through: add type predicate if :as is set on through
          if (as = through.options[:as])
            preds << "#{qt.call(through_tbl)}.#{qc.call("#{as}_type")} = " \
                     "#{conn.quote(model.name)}"
          end

          # through <-> source join predicate
          join_on =
            if source.macro == :belongs_to
              src_fk = source.foreign_key
              "#{qt.call(source_tbl)}.#{qc.call(source_pk)} = " \
              "#{qt.call(through_tbl)}.#{qc.call(src_fk)}"
            else
              src_fk =
                source.foreign_key ||
                reflection.options[:foreign_key] ||
                reflection.klass.reflections
                  .dig(source.name.to_s)&.foreign_key
              src_fk ||= source.foreign_key
              "#{qt.call(source_tbl)}.#{qc.call(src_fk)} = " \
              "#{qt.call(through_tbl)}.#{qc.call(through_pk)}"
            end

          <<-SQL.squish
            COALESCE((
              SELECT MAX(#{qt.call(source_tbl)}.#{qc.call(ts_col)})
              FROM #{qt.call(through_tbl)}
              INNER JOIN #{qt.call(source_tbl)} ON #{join_on}
              WHERE #{preds.map { |p| "(#{p})" }.join(" AND ")}
            ), #{epoch_literal(conn)})
          SQL
        end

        # ----------------------------- Helpers -----------------------------

        # Pick a timestamp column for a given ActiveRecord class.
        # Prefer "updated_at", fallback to "created_at", else nil.
        def dep_timestamp_column(klass)
          return nil unless klass.respond_to?(:column_names)

          cols = klass.column_names
          return "updated_at" if cols.include?("updated_at")
          return "created_at" if cols.include?("created_at")
          nil
        end

        def timestamp_column_for_model(model)
          dep_timestamp_column(model)
        end

        # Adapter-agnostic "greatest":
        # - PostgreSQL -> GREATEST(a, b, c)
        # - SQLite/others -> MAX(a, b, c)
        # Accepts either a variadic list of Arel nodes or an array.
        def greatest_arel(conn, *parts)
          exprs = parts.flatten.compact
          return (exprs.first || epoch_arel(conn)) if exprs.length <= 1

          Arel::Nodes::NamedFunction.new(greatest_function_name(conn), exprs)
        end

        def fn_coalesce(_conn) = "COALESCE"

        # Adapter-agnostic "epoch" as an Arel node.
        # - PostgreSQL -> CAST('1970-01-01 00:00:00' AS TIMESTAMP)
        # - SQLite (default in tests) -> DATETIME('1970-01-01 00:00:00')
        def epoch_arel(conn)
          if conn.adapter_name =~ /postgres/i
            Arel::Nodes::NamedFunction.new(
              "CAST", [Arel.sql("'1970-01-01 00:00:00' AS TIMESTAMP")]
            )
          else
            Arel::Nodes::NamedFunction.new(
              "DATETIME", [Arel.sql("'1970-01-01 00:00:00'")]
            )
          end
        end

        def greatest_function_name(conn)
          adapter = conn.adapter_name.to_s.downcase
          adapter.include?("postgres") ? "GREATEST" : "MAX"
        end

        # String literal of the epoch for raw SQL fragments.
        def epoch_literal(conn)
          adapter = conn.adapter_name.to_s.downcase
          if adapter.include?("postgres")
            "TIMESTAMP '1970-01-01 00:00:00'"
          else
            "DATETIME('1970-01-01 00:00:00')"
          end
        end

        # Unscoped arel_table helper.
        def arel_table(model)
          model.unscoped.arel_table
        end
      end
    end
  end
end
