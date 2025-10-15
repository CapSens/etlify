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
