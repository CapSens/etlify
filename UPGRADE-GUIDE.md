# UPGRADING FROM 0.11.0 -> 0.11.1

## 1. Overview

Etlify 0.11.1 is a bugfix release for `AirtableV0Adapter#batch_upsert!` when
the configured `id_property` is an Airtable **field ID** (e.g.
`"fldXXXXXXXXXXXXXX"`) rather than a field name.

**Bug fix:**

- `AirtableV0Adapter#batch_upsert!` now passes `returnFieldsByFieldId: true`
  in the `performUpsert` request when `id_property` matches the Airtable
  field ID format (`fld` followed by 14 alphanumeric characters). Without
  this flag, Airtable returned response fields keyed by name while
  `extract_batch_mapping` looked them up by ID, producing an empty mapping.
  Records were still upserted in Airtable, but `BatchSynchronizer` then
  persisted `crm_id: nil` on the matching `crm_synchronisations` row,
  leaving the sync stuck (subsequent runs hit `:not_modified` due to the
  digest match).

---

## 2. Database migrations

No database migration required for this upgrade.

---

## 3. Configuration changes

No configuration changes required. The flag is added conditionally based on
the `fld` prefix, preserving backward compatibility with field-name usage.

---

## 4. Recovering stuck records (if applicable)

If you ran 0.11.0 (or earlier) with `AirtableV0Adapter` and a field-ID
`id_property`, some `crm_synchronisations` rows may have `crm_id: nil`
together with a `last_digest`. Those records will not be re-synced
automatically because the digest still matches.

To recover them after upgrading, reset the digest on the affected rows so
they are picked up by the next `StaleRecords::BatchSync`:

```ruby
CrmSynchronisation
  .where(crm_name: "airtable", crm_id: nil)
  .where.not(last_digest: nil)
  .update_all(last_digest: nil)
```

The next batch sync will re-run `batch_upsert!` and now correctly populate
`crm_id`.

---

## 5. QA & testing checklist

- [ ] `bundle exec rspec` passes.
- [ ] In a non-production environment with an Airtable field-ID
      `id_property`, run `BatchSync` and confirm `crm_synchronisations.crm_id`
      is populated for every synced record.
- [ ] Existing setups using Airtable **field names** as `id_property` keep
      working unchanged (no `returnFieldsByFieldId` flag is sent).

---

## 6. Backward compatibility

Fully backward-compatible. The `returnFieldsByFieldId` flag is only added
when `id_property` matches the Airtable field-ID pattern (`fld` + 14
alphanumeric characters).

---

# UPGRADING FROM 0.10.0 -> 0.11.0

## 1. Overview

Etlify 0.11.0 adds an `enabled:` flag on `Etlify::CRM.register` that lets you
turn a CRM integration into a pure no-op at the process level — typically to
keep Etlify dormant in development or test environments while leaving other
CRMs active.

**New features:**

- **`enabled:` option** on `Etlify::CRM.register` (default `true`).
- **`Etlify::CRM.enabled?(name)`** public helper (returns `true` for unknown
  CRMs as a safe default).
- **Model API stays truthy when disabled** — `record.hubspot_sync!` and
  `record.hubspot_delete!` return `true` so calling code that branches on the
  return value keeps working unchanged.

---

## 2. Database migrations

No database migration required for this upgrade.

---

## 3. Configuration changes (optional)

Disable a CRM entirely, typically outside of production:

```ruby
Etlify::CRM.register(
  :hubspot,
  adapter: Etlify::Adapters::HubspotV3Adapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  ),
  enabled: Rails.env.production? || Rails.env.staging?,
  options: { job_class: Etlify::SyncJob }
)

Etlify::CRM.register(
  :another_crm,
  adapter: AnotherAdapter.new,
  # enabled: true  # default
)
```

`enabled:` must be a boolean — anything else raises `ArgumentError` at
registration time. Omitting the option keeps the previous behaviour
(`enabled: true`), so existing setups work unchanged.

---

## 4. Behaviour when a CRM is disabled

| Entry point | Return value | Side effects |
| --- | --- | --- |
| `record.<crm>_sync!` / `record.crm_sync!` | `true` | No job enqueued, no adapter call |
| `record.<crm>_delete!` / `record.crm_delete!` | `true` | No adapter call |
| `Etlify::Synchronizer.call` | `:disabled` | No adapter call, no write to `crm_synchronisations` |
| `Etlify::Deleter.call` | `:disabled` | No adapter call |
| `Etlify::BatchSynchronizer.call` | stats hash with `disabled: true`, `skipped: records.size` | No adapter call |
| `Etlify::StaleRecords::BatchSync.call` | disabled CRMs silently skipped, enabled CRMs processed normally | Stats only reflect enabled CRMs |

Existing `crm_synchronisations` rows are left untouched when a CRM is
disabled — flipping `enabled:` back to `true` resumes normal sync without any
extra action (records that became stale in the meantime will be picked up by
the next `BatchSync`).

---

## 5. QA & testing checklist

- [ ] In a non-production environment, set `enabled: false` on your CRM
      registration and confirm:
  - `record.hubspot_sync!` returns `true` and does not enqueue a job.
  - `record.hubspot_delete!` returns `true`.
  - `CrmSynchronisation.where(crm_name: "hubspot")` is not written.
  - No outgoing HTTP request to the CRM (mock adapter or network logs).
- [ ] In a spec, toggle `enabled` for a single CRM and assert other CRMs
      keep syncing through `Etlify::StaleRecords::BatchSync`.
- [ ] Re-enable the CRM and confirm that stale records are synced on the
      next run.

---

## 6. Backward compatibility

Fully backward-compatible. The default `enabled: true` preserves the
pre-0.11.0 behaviour for every existing CRM registration.

---

# UPGRADING FROM 0.9.4 -> 0.10.0

## 1. Overview

Etlify 0.10.0 introduces batch synchronization with built-in rate limiting:

**New features:**

- **`BatchSyncJob`** — A single job per CRM replaces N individual `SyncJob` instances for batch sync
- **`RateLimiter`** — Sleep-based rate limiting installed permanently on the adapter at `CRM.register` time
- **`BatchSynchronizer`** — Batch-aware synchronizer using `adapter.batch_upsert!` (100 records/request for HubSpot)
- **HubSpot batch operations** — `batch_upsert!` and `batch_delete!` via native batch endpoints
- **`DefaultHttp`** — Shared HTTP client extracted for adapter reuse

---

## 2. Database migrations

No database migration required for this upgrade.

---

## 3. Configuration changes

### 3.1. Rate limiting (recommended)

Add `rate_limit` to your CRM registration to enable automatic throttling:

```ruby
Etlify::CRM.register(
  :hubspot,
  adapter: Etlify::Adapters::HubspotV3Adapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  ),
  options: {
    rate_limit: { max_requests: 100, period: 10 },
    max_sync_errors: 5,
  }
)
```

The rate limiter is installed permanently on the adapter. **All sync paths are throttled**: `BatchSyncJob`, individual `SyncJob`, inline `crm_sync!(async: false)`, and pending sync flushes.

Without `rate_limit`, no throttling is applied (current behaviour preserved).

### 3.2. Custom `job_class` (still supported)

The `job_class` option on `CRM.register` and model DSL is still supported. Custom job classes will also benefit from rate limiting since the rate limiter lives on the adapter, not on the job.

---

## 4. Batch sync changes

### 4.1. `BatchSync.call(async: true)` behaviour change

**Before:** Enqueues one `SyncJob` per stale record (N jobs).

**After:** Enqueues one `BatchSyncJob` per CRM with all stale record pairs.

This is **not a breaking change** — the API is identical, only the internal job dispatch changed. The return value (`{total:, per_model:, errors:}`) is unchanged.

### 4.2. `BatchSyncJob` concurrency lock

A cache-based lock ensures only one `BatchSyncJob` per CRM runs at a time. If a second `BatchSyncJob` is enqueued for the same CRM while one is running, it is silently dropped.

### 4.3. `batch_upsert!` on adapters

If the adapter supports `batch_upsert!` (HubSpot, NullAdapter), `BatchSyncJob` uses `BatchSynchronizer` to group records and call `batch_upsert!` (up to 100 records per API request for HubSpot). If the adapter does not support `batch_upsert!`, it falls back to sequential `Synchronizer.call` per record.

---

## 5. Custom adapter updates (if applicable)

If you have a custom adapter and want to benefit from rate limiting, add a `rate_limiter=` accessor and call `@rate_limiter&.throttle!` before each HTTP request:

```ruby
class MyCrmAdapter
  attr_accessor :rate_limiter

  private

  def request(method, path, body: nil)
    @rate_limiter&.throttle!
    # ... perform HTTP request
  end
end
```

If you also want batch support, implement `batch_upsert!` returning a `Hash{id_property_value => crm_id}`:

```ruby
def batch_upsert!(object_type:, records:, id_property:)
  # ... call CRM batch API
  # return { "john@example.com" => "123", "jane@example.com" => "456" }
end
```

---

## 6. QA & testing checklist

- [ ] `rate_limit` configured in initializer for each CRM
- [ ] `BatchSync.call(async: true)` enqueues `BatchSyncJob` (not N `SyncJob`)
- [ ] Individual `model.crm_sync!` still works and is rate-limited
- [ ] `bundle exec rspec` passes
- [ ] No 429 errors in production logs after deployment

---

## 7. Backward compatibility

All changes are backward compatible:

- `SyncJob` is kept for individual `model.crm_sync!` calls
- `BatchSync.call(async: false)` (inline mode) is unchanged
- Adapters without `rate_limiter=` or `batch_upsert!` continue to work
- `rate_limit` is optional — no throttle when absent

---

# UPGRADING FROM 0.9.3 -> 0.9.4

## 1. Overview

Etlify 0.9.4 introduces three new features and two bug fixes:

**New features:**

- **`stale_scope`** — Restrict which records the `StaleRecords::Finder` considers at SQL level
- **`error_count`** — Track consecutive sync failures and auto-exclude broken records
- **`sync_dependencies`** — Buffer syncs until dependencies have a `crm_id`

**Bug fixes:**

- `StaleRecords::Finder` now handles `has_one :through` dependencies where the through association is a `belongs_to`
- `StaleRecords::Finder` now handles STI subclasses without `PG::UndefinedColumn` errors

---

## 2. Database migrations (required)

### 2.1. Add `error_count` column

```bash
rails g etlify:add_error_count
rails db:migrate
```

This adds an `error_count` integer column (default: `0`) to `crm_synchronisations`.

### 2.2. Create `etlify_pending_syncs` table

Only required if you plan to use `sync_dependencies`:

```bash
rails g etlify:migration create_etlify_pending_syncs
rails db:migrate
```

---

## 3. Configuration (optional)

### 3.1. `max_sync_errors`

Records exceeding this limit are automatically excluded from `StaleRecords::Finder`.
Default: `3`.

```ruby
# Global
Etlify.configure do |config|
  config.max_sync_errors = 5
end

# Or per-CRM
Etlify::CRM.register(
  :hubspot,
  adapter: Etlify::Adapters::HubspotV3Adapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  ),
  options: { max_sync_errors: 5 }
)
```

To manually re-enable sync on a record after fixing the root cause:

```ruby
CrmSynchronisation.find(id).reset_error_count!
```

---

## 4. New DSL options (optional)

### 4.1. `stale_scope`

Restricts which records `StaleRecords::Finder` considers. Applied at SQL level
before any record is processed. Prevents unnecessary `CrmSynchronisation` rows
for records that `sync_if` would skip.

```ruby
class User < ApplicationRecord
  include Etlify::Model

  hubspot_etlified_with(
    serializer: Etlify::Serializers::UserSerializer,
    crm_object_type: "contacts",
    id_property: "email",
    stale_scope: ->(rel) { rel.where(active: true) }
  )
end
```

Models without `stale_scope` are not affected — the Finder behaves as before.

### 4.2. `sync_dependencies`

Buffers sync when a dependency has no `crm_id` yet. Automatically retries once
the dependency is synced. Supports both etlified models (via `CrmSynchronisation`)
and legacy models with a direct `#{crm_name}_id` column.

```ruby
class Employee < ApplicationRecord
  include Etlify::Model

  hubspot_etlified_with(
    serializer: Etlify::Serializers::EmployeeSerializer,
    crm_object_type: "contacts",
    id_property: "email",
    dependencies: [:company],
    sync_dependencies: [:company]
  )
end
```

> **Note:** `dependencies` controls freshness checks (re-sync when dependency
> changes). `sync_dependencies` controls ordering (block until dependency has a
> `crm_id`). They can overlap but serve different purposes.

---

## 5. QA & testing checklist

- [ ] Migration adds `error_count` to `crm_synchronisations`
- [ ] Migration creates `etlify_pending_syncs` table (if using `sync_dependencies`)
- [ ] Records with `error_count >= max_sync_errors` are excluded from `StaleRecords::Finder`
- [ ] `CrmSynchronisation#reset_error_count!` re-enables sync
- [ ] `stale_scope` correctly restricts the Finder query
- [ ] `sync_dependencies` buffers and flushes correctly
- [ ] STI models sync without `PG::UndefinedColumn` errors
- [ ] `has_one :through` (via `belongs_to`) dependencies are detected as stale

---

## 6. Backward compatibility

All features are backward compatible. Existing code continues to work without
changes. The `error_count` migration is strongly recommended to avoid retrying
permanently broken records.

---

# UPGRADING FROM 0.9.2 -> 0.9.3

- Nothing to do (bugfix: custom `job_class` support in `BatchSync` via CRM options)

# UPGRADING FROM 0.9.1 -> 0.9.2

- Nothing to do (bugfix)

# UPGRADING FROM 0.9.0 -> 0.9.1

- Nothing to do (bugfix)

# UPGRADING FROM 0.8.x -> 0.9.0

⚠️ **Breaking changes ahead.**

---

## 1. Overview

Etlify 0.9.0 introduces **multi-CRM support** and requires a `crm_name` column
in your `crm_synchronisations` table. Jobs and model DSLs have also evolved.

**Key changes:**

- Multi-CRM support via `Etlify::CRM.register(:hubspot, ...)`
- Each sync line is now scoped by `crm_name`
- Updated job signature: `perform(model, id, crm_name)`
- Model DSLs renamed to `<crm>_etlified_with(...)`

---

## 2. Initializer setup

Update `config/initializers/etlify.rb`:

```ruby
require "etlify"

Etlify.configure do |config|
  Etlify::CRM.register(
    :hubspot,
    adapter: Etlify::Adapters::HubspotV3Adapter.new(
      access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
    ),
    options: { job_class: "Etlify::SyncObjectWorker" }
  )

  # Default values (optional)
  # config.digest_strategy = Etlify::Digest.method(:stable_sha256)
  # config.job_queue_name = "low"
  # config.cache_store = Rails.cache || ActiveSupport::Cache::MemoryStore.new
end
```

---

## 3. Database migration

Add the new `crm_name` column and rebuild the unique index.

```ruby
class AddCrmNameToCrmSynchronisations < ActiveRecord::Migration[7.2] # Change with your version
  def self.up
    add_column :crm_synchronisations, :crm_name, :string
    add_index :crm_synchronisations, :crm_name

    remove_index(
      :crm_synchronisations,
      [:resource_type, :resource_id],
      unique: true,
      name: "idx_crm_sync_on_resource"
    )
    add_index(
      :crm_synchronisations,
      [:resource_type, :resource_id, :crm_name],
      unique: true,
      name: "idx_crm_sync_on_resource"
    )

    # Set default crm_name to 'hubspot' for existing records
    execute <<-SQL.squish
      UPDATE crm_synchronisations
      SET crm_name = 'hubspot'
      WHERE crm_name IS NULL
    SQL

    change_column_null :crm_synchronisations, :crm_name, false
  end

  def self.down
    remove_index :crm_synchronisations, :crm_name
    remove_column :crm_synchronisations, :crm_name

    remove_index(
      :crm_synchronisations,
      [:resource_type, :resource_id, :crm_name],
      unique: true,
      name: "idx_crm_sync_on_resource"
    )
    add_index(
      :crm_synchronisations,
      [:resource_type, :resource_id],
      unique: true,
      name: "idx_crm_sync_on_resource"
    )
  end
end
```

> **Tip:** If you already have duplicates on `(resource_type, resource_id)`,
> deduplicate before applying the unique index.

---

## 4. Model configuration (new DSL)

Each model must now declare its CRM configuration explicitly.

```ruby
class User < ApplicationRecord
  include Etlify::Model

  hubspot_etlified_with(
    serializer: Etlify::Serializers::UserSerializer,
    crm_object_type: "contacts",
    id_property: "email",
    dependencies: [:company],
    sync_if: ->(record) { record.email.present? }
  )
end
```

You can declare multiple CRMs per model by repeating the macro.

---

## 5. Job updates

### Sidekiq (plain worker)

```ruby
module Etlify
  class SyncObjectWorker
    include Sidekiq::Worker

    sidekiq_options(
      retry: false,
      queue: :low,
      lock: :until_executed,
      lock_timeout: 0,
      lock_args_method: :lock_args
    )

    def perform(model_name, id, crm_name)
      model = model_name.constantize
      record = model.find_by(id: id)
      return unless record

      Etlify::Synchronizer.call(record, crm_name: crm_name.to_sym)
    end

    def self.lock_args(*args)
      [*args]
    end
  end
end
```

### Manual checks in Rails console

```ruby
CrmSynchronisation.where(crm_name: nil).count
CrmSynchronisation.group(:crm_name).count
CrmSynchronisation.find_by(crm_name: "hubspot")
User.first&.crm_sync!(crm_name: :hubspot, async: false)
```

---

## 7. Admin & UI updates

- Add `crm_name` to admin views or dashboards.
- Update any filters, exports, or queries to include `crm_name`.

---

## 8. QA & testing checklist

- [ ] Migration adds `crm_name` and backfills existing rows.
- [ ] Unique index `(resource_type, resource_id, crm_name)` exists.
- [ ] Finder and BatchSync correctly filter by `crm_name`.
- [ ] Jobs receive `crm_name` argument.
- [ ] Serializers output expected payloads.
- [ ] Console call works: `User.first.crm_sync!(crm_name: :hubspot, async: false)`.

---

## 9. Rollback plan

The `down` method in the migration restores the old schema.
If data deduplication was done, ensure a CSV export exists before running down.

---

**Happy syncing! 🚀**
