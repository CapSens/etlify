module Etlify
  module StaleRecords
    # Finder builds, for each configured model/CRM, an ids-only relation
    # containing records that are considered "stale" and need to be synced.
    #
    # Return shape:
    #   {
    #     User  => { hubspot: <ActiveRecord::Relation>, salesforce: <...> },
    #     Company => { hubspot: <...> }
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
          return [] unless model.respond_to?(:etlify_crms) && model.etlify_crms.present?

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
        #   - SELECT "<owners>.<pk> AS id" and ORDER BY the same
        #   - Wrap as subquery and expose ids as a simple "id" column to keep
        #     pluck(:id) and batching unambiguous on all adapters.
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

          # threshold = greatest(owner.updated_at, deps.updated_at..., epoch)
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

          # Base relation (with JOIN) selecting only the owner's PK, aliased as "id".
          base_rel =
            model.unscoped
                 .from(owner_arel)
                 .joins(join_sql)
                 .where(where_pred)
                 .select(Arel.sql("#{qualified_pk_sql} AS id"))
                 .reorder(Arel.sql("#{qualified_pk_sql} ASC"))

          # Wrap the base relation as a subquery so that pluck(:id) is unambiguous.
          sub_sql  = base_rel.to_sql
          sub_from = Arel.sql("(#{sub_sql}) AS etlify_stale_ids")

          model.unscoped
               .from(sub_from)
               .select(Arel.sql("id"))
               .reorder(Arel.sql("id ASC"))
        end

        # ---------- Threshold (owner + dependencies) ----------

        # Build an Arel expression representing the newest timestamp among:
        #   - owner's updated_at
        #   - each configured dependency's newest updated_at
        #   - epoch fallback when values are NULL or missing
        def latest_timestamp_arel(model, crm_name:, conn:)
          owner_arel = arel_table(model)
          parts = []

          # Owner updated_at (NULL -> epoch)
          parts << Arel::Nodes::NamedFunction.new(
            fn_coalesce(conn), [owner_arel[:updated_at], epoch_arel(conn)]
          )

          deps =
            Array(model.etlify_crms.dig(crm_name.to_sym, :dependencies))
              .map(&:to_sym)

          deps.each do |dep_name|
            reflection = model.reflect_on_association(dep_name)
            next unless reflection

            parts << dependency_max_timestamp_arel(model, reflection, conn)
          end

          # Adapter-aware greatest
          greatest_arel(conn, *parts)
        end

        # Dispatch to the proper dependency strategy.
        def dependency_max_timestamp_arel(model, reflection, conn)
          # 1) Owner belongs_to polymorphic (e.g., User belongs_to :avatarable)
          if reflection.polymorphic? && reflection.macro == :belongs_to
            return Arel.sql(
              owner_polymorphic_belongs_to_timestamp_sql(model, reflection, conn)
            )
          end

          # 2) Through associations
          if reflection.through_reflection
            return Arel.sql(
              through_dependency_timestamp_sql(model, reflection, conn)
            )
          end

          # 3) HABTM
          if reflection.macro == :has_and_belongs_to_many
            return Arel.sql(
              habtm_dependency_timestamp_sql(model, reflection, conn)
            )
          end

          # 4) Direct associations
          direct_dependency_timestamp_arel(model, reflection, conn)
        end

        # ---------- Direct associations (belongs_to / has_one / has_many) ---

        def direct_dependency_timestamp_arel(model, reflection, conn)
          owner_arel = arel_table(model)

          case reflection.macro
          when :belongs_to
            dep_arel = reflection.klass.arel_table
            fk       = reflection.foreign_key

            sub =
              dep_arel.project(dep_arel[:updated_at])
                      .where(dep_arel[reflection.klass.primary_key].eq(owner_arel[fk]))
                      .take(1)

            Arel::Nodes::NamedFunction.new(
              fn_coalesce(conn),
              [Arel::Nodes::Grouping.new(sub), epoch_arel(conn)]
            )

          when :has_one, :has_many
            dep_arel = reflection.klass.arel_table
            fk       = reflection.foreign_key

            preds = []
            preds << dep_arel[fk].eq(owner_arel[model.primary_key])

            # Support polymorphic :as
            if (poly_as = reflection.options[:as])
              type_col = "#{poly_as}_type"
              preds << dep_arel[type_col].eq(model.name)
            end

            sub =
              dep_arel.project(
                        Arel::Nodes::NamedFunction.new("MAX", [dep_arel[:updated_at]])
                      )
                      .where(preds.reduce(&:and))

            Arel::Nodes::NamedFunction.new(
              fn_coalesce(conn),
              [Arel::Nodes::Grouping.new(sub), epoch_arel(conn)]
            )

          else
            # Unknown macro: epoch fallback
            epoch_arel(conn)
          end
        end

        # ----------------------------- HABTM -------------------------------

        # Build MAX(updated_at) over the HABTM source table joined via the join table.
        def habtm_dependency_timestamp_sql(model, reflection, conn)
          owner_tbl  = model.table_name
          source_tbl = reflection.klass.table_name
          source_pk  = reflection.klass.primary_key
          source_qt  = conn.quote_table_name(source_tbl)
          source_uc  = conn.quote_column_name("updated_at")

          join_tbl = reflection.join_table.to_s
          jt_qt    = conn.quote_table_name(join_tbl)

          owner_fk  = reflection.foreign_key.to_s
          source_fk = reflection.association_foreign_key.to_s

          preds = []
          preds << "#{jt_qt}.#{conn.quote_column_name(owner_fk)} = " \
                   "#{conn.quote_table_name(owner_tbl)}." \
                   "#{conn.quote_column_name(model.primary_key)}"
          preds_sql = preds.map { |p| "(#{p})" }.join(" AND ")

          <<-SQL.squish
            COALESCE((
              SELECT MAX(#{source_qt}.#{source_uc})
              FROM #{jt_qt}
              INNER JOIN #{source_qt}
                ON #{source_qt}.#{conn.quote_column_name(source_pk)} =
                   #{jt_qt}.#{conn.quote_column_name(source_fk)}
              WHERE #{preds_sql}
            ), #{epoch_literal(conn)})
          SQL
        end

        # -------------------------- has_* :through -------------------------

        def through_dependency_timestamp_sql(model, reflection, conn)
          through    = reflection.through_reflection
          source     = reflection.source_reflection

          through_tbl = through.klass.table_name
          through_pk  = through.klass.primary_key
          source_tbl  = reflection.klass.table_name
          source_pk   = reflection.klass.primary_key
          owner_tbl   = model.table_name

          qt = ->(t) { conn.quote_table_name(t) }
          qc = ->(c) { conn.quote_column_name(c) }

          preds = []
          preds << "#{qt.call(through_tbl)}.#{qc.call(through.foreign_key)} = " \
                   "#{qt.call(owner_tbl)}.#{qc.call(model.primary_key)}"

          # Polymorphic through: add type predicate
          if (as = through.options[:as])
            preds << "#{qt.call(through_tbl)}.#{qc.call("#{as}_type")} = " \
                     "#{conn.quote(model.name)}"
          end

          join_on =
            if source.macro == :belongs_to
              "#{qt.call(source_tbl)}.#{qc.call(source_pk)} = " \
              "#{qt.call(through_tbl)}.#{qc.call(source.foreign_key)}"
            else
              "#{qt.call(source_tbl)}.#{qc.call(source.foreign_key)} = " \
              "#{qt.call(through_tbl)}.#{qc.call(through_pk)}"
            end

          <<-SQL.squish
            COALESCE((
              SELECT MAX(#{qt.call(source_tbl)}.#{qc.call("updated_at")})
              FROM #{qt.call(through_tbl)}
              INNER JOIN #{qt.call(source_tbl)} ON #{join_on}
              WHERE #{preds.map { |p| "(#{p})" }.join(" AND ")}
            ), #{epoch_literal(conn)})
          SQL
        end

        # ---------------- Owner belongs_to polymorphic ---------------------

        # Compute newest timestamp of the concrete target referenced by the owner's
        # polymorphic belongs_to (e.g., users.avatarable_type/_id).
        # We avoid reflection.klass here and derive concrete tables from *_type values.
        def owner_polymorphic_belongs_to_timestamp_sql(model, reflection, conn)
          owner_tbl = model.table_name
          fk        = reflection.foreign_key
          type_col  = reflection.foreign_type

          types = model.distinct.pluck(type_col).compact.uniq
          return epoch_literal(conn) if types.empty?

          parts = types.filter_map do |type_name|
            klass = safe_constantize(type_name)
            next nil unless klass&.respond_to?(:table_name)

            dep_tbl = klass.table_name
            dep_pk  = klass.primary_key

            <<-SQL.squish
              COALESCE((
                SELECT #{conn.quote_table_name(dep_tbl)}.
                       #{conn.quote_column_name("updated_at")}
                FROM #{conn.quote_table_name(dep_tbl)}
                WHERE #{conn.quote_table_name(owner_tbl)}.
                      #{conn.quote_column_name(type_col)} = #{conn.quote(type_name)}
                  AND #{conn.quote_table_name(dep_tbl)}.
                      #{conn.quote_column_name(dep_pk)} =
                      #{conn.quote_table_name(owner_tbl)}.
                      #{conn.quote_column_name(fk)}
                LIMIT 1
              ), #{epoch_literal(conn)})
            SQL
          end

          return parts.first if parts.size == 1

          fn = greatest_function_name(conn)
          "#{fn}(#{parts.join(', ')})"
        end

        # ----------------------------- Arel helpers ------------------------

        # Adapter-agnostic "greatest":
        # - PostgreSQL -> GREATEST(a, b, c)
        # - SQLite/others -> MAX(a, b, c)
        # Accepts either a variadic list of Arel nodes or an array.
        # If a single expression is given, returns it and ensures it responds to #to_sql
        # (for specs that call .to_sql on the single node).
        def greatest_arel(conn, *parts)
          exprs = parts.flatten.compact
          return ensure_to_sql_node(exprs.first) if exprs.length <= 1

          Arel::Nodes::NamedFunction.new(greatest_function_name(conn), exprs)
        end

        # Provide an Arel node that responds to #to_sql, even for SqlLiteral.
        def ensure_to_sql_node(node)
          return node if node.respond_to?(:to_sql)
          node.define_singleton_method(:to_sql) { to_s }
          node
        end

        def fn_coalesce(_conn) = "COALESCE"

        # Adapter-agnostic "epoch" as an Arel node that responds to #to_sql.
        # - PostgreSQL -> CAST('1970-01-01 00:00:00' AS TIMESTAMP)
        # - SQLite (default in tests) -> DATETIME('1970-01-01 00:00:00')
        def epoch_arel(conn)
          if conn.adapter_name =~ /postgres/i
            Arel::Nodes::NamedFunction.new(
              "CAST",
              [Arel.sql("'1970-01-01 00:00:00' AS TIMESTAMP")]
            )
          else
            Arel::Nodes::NamedFunction.new(
              "DATETIME",
              [Arel.sql("'1970-01-01 00:00:00'")]
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

        # Unscoped arel_table helper (kept public-ish for debugging if needed).
        def arel_table(model)
          model.unscoped.arel_table
        end

        # Safe constantize (ignore missing constants).
        def safe_constantize(str)
          str.constantize
        rescue NameError
          nil
        end
      end
    end
  end
end
