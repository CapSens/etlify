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
