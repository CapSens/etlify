# Etlify

> Rails-first, idempotent synchronisation between your ActiveRecord models and your CRM. HubSpot is supported out of the box; other CRMs can be plugged in via adapters.

This gem has been designed by [Capsens, a fintech web and mobile agency based in Paris](https://capsens.eu/).

---

## Why Etlify? (Context & Intended Use)

In internal products, it is common to persist domain data in Rails while also mirroring a subset of it into a CRM for marketing, sales or support workflows. Etlify provides a small, dependable toolkit to **declare** which models are CRM-backed, **serialise** them into CRM payloads, and **synchronise** them in an **idempotent** fashion so repeated calls are safe and efficient.

Etlify sits beside your app; it does **not** try to own your domain or background processing. It integrates naturally with ActiveRecord and ActiveJob so you keep your current architecture and simply “switch on” CRM sync where you need it.

---

## Features at a glance

| Area        | What you get                                                  | Why it helps                                        |
| ----------- | ------------------------------------------------------------- | --------------------------------------------------- |
| DSL         | `include Etlify::Model` + `etlified_with(...)` on your models | Opt-in sync with a single line; clear, local intent |
| Serialisers | A base class to turn a model into a CRM payload               | Keeps mapping logic where it belongs; easy to test  |
| Adapters    | HubSpot & Airtable adapters included; plug your own           | Swap CRMs without touching model code               |
| Idempotence | Stable digest of the last synced payload                      | Avoids redundant API calls; safe to retry           |
| Jobs        | `crm_sync!` enqueues an ActiveJob; batch sync via `BatchSyncJob` | Fits your queue; built-in rate limiting              |
| Delete      | `crm_delete!` to remove a record from the CRM                 | Keeps both sides consistent                         |

---

## Requirements & Compatibility

- **Ruby:** 3.0+
- **Rails:** 6.1+ (ActiveRecord & ActiveJob)
- **Datastore:** A relational database supported by ActiveRecord for storing sync state.
- **Threading/Jobs:** Any ActiveJob backend (Sidekiq, Delayed Job, etc.).

> These ranges reflect typical modern Rails setups. If you run older stacks, test in your environment.

---

## Installation

Add the gem to your application:

```ruby
# Gemfile
gem "etlify"
```

Then install and run the generators:

```bash
bundle install

# Install initializer(s)
bin/rails generate etlify:install

# Install sync-state tables
bin/rails generate etlify:migration CreateCrmSynchronisations
bin/rails generate etlify:migration create_etlify_pending_syncs
bin/rails db:migrate

# Generate a serializer for a model (optional helper)
bin/rails generate etlify:serializer User
# => creates app/serializers/etlify/user_serializer.rb
```

> You may create your own serializer class manually as long as it responds to `#new(record)` and `#as_crm_payload`.

---

## Configuration

Create `config/initializers/etlify.rb`:

```ruby
# config/initializers/etlify.rb
require "etlify"

Etlify.configure do |config|
  Etlify::CRM.register(
    :hubspot,
    adapter: Etlify::Adapters::HubspotV3Adapter.new(
      access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
    ),
    options: {
      job_class: "Etlify::SyncObjectWorker",
      max_sync_errors: 5
    }
  )
  # will provide DSL below for models
  # hubspot_etlified_with(...)

  # Etlify::CRM.register(
  #   :another_crm, adapter: Etlify::Adapters::AnotherAdapter,
  #   options: { job_class: Etlify::SyncJob }
  # )
  # will provide DSL below for models
  # another_crm_etlified_with(...)

  # @digest_strategy = Etlify::Digest.method(:stable_sha256)
  # @job_queue_name = "low"
end
```

### Declaring a CRM-synced model

```ruby
# app/models/user.rb
class User < ApplicationRecord
  include Etlify::Model

  has_many :investments, dependent: :destroy

  hubspot_etlified_with(
    serializer: UserSerializer,
    crm_object_type: "contacts",
    id_property: :id,
    # Only sync when an email exists
    sync_if: ->(user) { user.email.present? },
    # useful if your object serialization includes dependencies
    dependencies: [:investments],
    # buffer sync until these associations have a crm_id
    sync_dependencies: [:users_profile]
  )
end
```

#### Restricting the Finder scope with `stale_scope`

By default, the `StaleRecords::Finder` scans **all** records of an etlified model.
If only a subset of records should ever be synced (e.g. only `marketplace` operations),
you can pass a `stale_scope` lambda that returns an ActiveRecord scope:

```ruby
class Trading::Operation < ApplicationRecord
  include Etlify::Model

  scope :marketplace, -> { where(category: "marketplace") }

  hubspot_etlified_with(
    serializer: TradingOperationSerializer,
    crm_object_type: "deals",
    id_property: :id,
    sync_if: ->(op) { op.marketplace? },
    stale_scope: -> { marketplace }
  )
end
```

#### Limiting automatic retries with `max_sync_errors`

When a record fails to sync repeatedly (CRM misconfigured, server error, etc.),
Etlify increments an `error_count` on its `CrmSynchronisation` row. Once the
count reaches the configured limit, the record is **excluded** from
`StaleRecords::Finder` automatic retries.

The default limit is **3**. You can change it globally or per CRM:

```ruby
# Global default (config/initializers/etlify.rb)
Etlify.configure do |config|
  config.max_sync_errors = 10
end

# Per-CRM override (takes precedence over global)
Etlify::CRM.register(
  :hubspot,
  adapter: Etlify::Adapters::HubspotV3Adapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  ),
  options: { max_sync_errors: 5 }
)
```

To re-enable sync after fixing the root cause:

```ruby
sync_line = CrmSynchronisation.find_by(
  resource: user, crm_name: "hubspot"
)
sync_line.reset_error_count!
```

> **Upgrading?** Run `rails g etlify:add_error_count && rails db:migrate`
> to add the `error_count` column.

---

#### Ordering sync with `sync_dependencies`

When a model's CRM payload references another model's `crm_id` (e.g. an Airtable record ID), that dependency must be synced **first**. The `sync_dependencies` option handles this automatically:

```ruby
class Trading::Operation < ApplicationRecord
  include Etlify::Model

  has_one :buyer_profile, through: :buyer, source: :profile

  airtable_etlified_with(
    serializer: TradingOperationSerializer,
    crm_object_type: "deals",
    id_property: :id,
    sync_dependencies: [:buyer_profile]
  )
end
```

**How it works:**

1. Before syncing, Etlify checks each `sync_dependency` association for a `crm_id`. It first looks in the `CrmSynchronisation` table (etlified models), then falls back to a direct `#{crm_name}_id` column on the model (legacy models, e.g. `airtable_id`).
2. If any dependency is missing a `crm_id`, the sync is **buffered**: an `Etlify::PendingSync` row is created and the dependency is enqueued for sync. The method returns `:buffered`.
3. Once the dependency is successfully synced (`:synced`), Etlify **flushes** all its pending dependents by re-enqueuing them via `crm_sync!`.

> **Note:** This requires the `etlify_pending_syncs` table. Run `rails g etlify:migration create_etlify_pending_syncs && rails db:migrate` if you haven't already.

### Writing a serializer

```ruby
# app/serializers/etlify/user_serializer.rb
class UserSerializer
  attr_accessor :user

  # your serializer must implement #intiialize(object) #and as_crm_payload
  def initialize(user)
    @user = user
  end

  # Must return a Hash that matches your CRM field names
  def as_crm_payload # or #to_h
    {
      email: user.email,
      firstname: user.first_name,
      lastname: user.last_name
    }
  end
end
```

---

## Usage

### Synchronise a single record

```ruby
user = User.find(1)

# Async by default (enqueues an Etlify::SyncJob by default)
# The job class can be overriden when registering the CRM
user.hubspot_crm_sync! # or user.#{registered_crm_name}_crm_sync!

# Run inline (no job)
user.crm_sync!(async: false)
```

### Delete a record from the CRM

```ruby
# Inline delete (not enqueued)
user.hubspot_crm_delete! # or user.#{registered_crm_name}_crm_delete!
```

### Custom serializer example

```ruby
# app/serializers/etlify/company_serializer.rb
class CompanySerializer
  attr_accessor :company

  def initialize(company)
    @company = company
  end

  # Keep serialisation small and predictable
  def as_crm_payload
    {
      name: company.name,
      domain: company.domain,
      hs_lead_status: company.lead_status
    }
  end
end
```

---

## Batch synchronisation

Beyond single-record sync, Etlify provides a **batch resynchronisation API** that targets **all “stale” records** (those whose data has changed since the last CRM sync). This is useful for:

- recovering from CRM or worker outages,
- triggering periodic re-syncs (cron jobs),
- testing/debugging your serialization logic on a controlled dataset.

### API

```ruby
# Enqueue (default): one BatchSyncJob per CRM
Etlify::StaleRecords::BatchSync.call

# Restrict to specific models
Etlify::StaleRecords::BatchSync.call(models: [User, Company])

# Restrict to a specific CRM
Etlify::StaleRecords::BatchSync.call(crm_name: :hubspot)

# Or both
Etlify::StaleRecords::BatchSync.call(
  crm_name: :hubspot,
  models: [User, Company]
)

# Run inline (no jobs), useful for scripts/maintenance or testing
Etlify::StaleRecords::BatchSync.call(async: false)

# Adjust SQL batch size (number of IDs per batch)
Etlify::StaleRecords::BatchSync.call(batch_size: 1_000)
```

**Return value**
The method returns a stats Hash:

```ruby
{
  total:     Integer,              # number of records processed (or counted)
  per_model: { “User” => 42, ...}, # per-model breakdown
  errors:    Integer               # number of errors in async:false mode
}
```

### How it works

- `Etlify::StaleRecords::Finder` scans all **etlified models**
  (those that called `#{crm_name}_etlified_with`) and builds, for each,
  a **SQL relation selecting only the PKs** of stale records.
- A record is considered stale if:
  - it **has no** `crm_synchronisation` row, **or**
  - its `last_synced_at` is **older** than the **max** `updated_at` among:
    - its own row,
    - and its declared dependencies (via `dependencies:` in `etlified_with`,
      supporting `belongs_to`, `has_one`, `has_many`, `has_* :through`,
      and polymorphic `belongs_to`).
- `Etlify::StaleRecords::BatchSync` then iterates **by ID batches**:
  - in **async: true** mode (default): collects all stale record IDs and
    enqueues a **single `BatchSyncJob` per CRM** with all the pairs. The job
    processes records sequentially, respecting the configured rate limit;
  - in **async: false** mode: load each record and pass it to
    `Etlify::Synchronizer.call(record)` **inline**
    (errors are logged and counted without interrupting the batch).

> Individual `model.crm_sync!` calls still use `SyncJob` (one job per record)
> for immediate, on-demand sync. `BatchSyncJob` is used only by `BatchSync`.

### Rate limiting

CRM APIs enforce rate limits (e.g. HubSpot: ~100 requests/10s, Airtable: 5 requests/s). Etlify provides built-in rate limiting at the **adapter level** (per HTTP request), so multi-request operations like search + upsert are correctly throttled.

#### Configuration

```ruby
Etlify::CRM.register(
  :hubspot,
  adapter: Etlify::Adapters::HubspotV3Adapter.new(
    access_token: ENV[“HUBSPOT_PRIVATE_APP_TOKEN”]
  ),
  options: {
    rate_limit: { max_requests: 100, period: 10 },
    max_sync_errors: 5,
  }
)
```

- `max_requests`: maximum number of HTTP requests allowed in the period.
- `period`: time window in seconds.

When `rate_limit` is not configured, no throttling is applied (current behaviour preserved).

#### How it works

1. At `CRM.register` time, if `rate_limit` is configured and the adapter supports `rate_limiter=`, a `RateLimiter` is **permanently installed** on the adapter.
2. Every HTTP request in the adapter calls `rate_limiter.throttle!`, which sleeps the minimum necessary time to stay within the rate limit.
3. **All sync paths are throttled**: `BatchSyncJob`, individual `SyncJob`, inline `crm_sync!(async: false)`, and pending sync flushes — they all go through the same adapter.
4. If the CRM returns a **429 (Rate Limited)** response despite throttling, `BatchSyncJob` re-enqueues itself with the **remaining records** after a backoff delay (default: 10 seconds).
5. A cache-based lock ensures only **one `BatchSyncJob` runs per CRM** at a time.

#### Custom adapter support

To support rate limiting in a custom adapter, add a `rate_limiter=` accessor and call `@rate_limiter&.throttle!` before each HTTP request:

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

## Best practices

- **Production**: prefer `async: true` with a **dedicated, low-priority queue**
  via ActiveJob.
- **Rate limits**: configure `rate_limit` per CRM to avoid 429 errors and
  let `BatchSyncJob` handle throttling automatically.
- **Stable payloads**: ensure your serializers produce deterministic Hashes to
  benefit from **idempotence**.
- **Dependencies**: declare `dependencies:` accurately in `etlified_with` so
  indirect changes trigger resyncs.
- **Batch size**: adjust `batch_size` to your DB to balance throughput and memory.

---

## How idempotence works

- Before sending anything to the CRM, Etlify builds the payload via your serializer and computes a **stable digest** (SHA-256 by default) of that payload.
- Etlify stores the **last successful digest** alongside the CRM ID for that record in your application database.
- On subsequent syncs, if the **new digest equals the last stored digest**, Etlify **skips** the remote call and returns `:not_modified`.
- If the digest **differs**, Etlify upserts the record remotely and updates the stored digest.

You can customise the digest strategy:

```ruby
Etlify.config.digest_strategy = lambda do |payload|
  # Always use deterministic JSON generation for hashing
  Digest::SHA256.hexdigest(JSON.dump(payload))
end
```

> Tip: Keep your serializer output **stable** (e.g. avoid unordered hashes or volatile timestamps) so that digests are meaningful.

---

## HubSpot adapter (API v3)

Etlify ships with `Etlify::Adapters::HubspotV3Adapter`. It supports native objects (e.g. **contacts**, **companies**, **deals**) and custom objects by API name.

### Configuration

```ruby
Etlify.configure do |config|
  Etlify::CRM.register(
    :hubspot,
    adapter: Etlify::Adapters::HubspotV3Adapter.new(
      access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
    ),
    options: {job_class: "Etlify::SyncObjectWorker"}
  )
end
```

### Behaviour

- `object_type`: the target entity, e.g. `"contacts"`, `"companies"`, `"deals"`, or the API name of a custom object.
- `id_property` (mandatory): if your upsert should search for an existing record by a unique property (e.g. `"email"` for contacts), the adapter uses it to find-or-create.
- If no match is found (or no `id_property` is provided), the adapter **creates** a new record.

### Example: Contact upsert

```ruby
class User < ApplicationRecord
  include Etlify::Model

  hubspot_etlified_with(
    serializer: UserSerializer,
    crm_object_type: "contacts",
    id_property: :email,
    sync_if: ->(user) { user.email.present? }
  )
end

# Later
user.hubspot_crm_sync! # Adapter performs an upsert
```

### Example: Custom object

```ruby
class Subscription < ApplicationRecord
  include Etlify::Model

  hubspot_etlified_with(
    serializer: SubscriptionSerializer,
    crm_object_type: "p1234567_subscription" # Custom object API name,
    id_propery: :id,
  )
end
```

### Batch operations

The HubSpot adapter provides two additional methods for bulk operations, using HubSpot's native batch endpoints (up to 100 inputs per request, auto-sliced):

```ruby
adapter = Etlify::CRM.registry[:hubspot].adapter

# Batch upsert via HubSpot's native /batch/upsert
# Returns an Array of hs_object_id strings
adapter.batch_upsert!(
  object_type: "contacts",
  records: [
    {email: "a@example.com", firstname: "Alice"},
    {email: "b@example.com", firstname: "Bob"},
  ],
  id_property: "email"
)

# Batch delete (archive) via /batch/archive
# Returns true
adapter.batch_delete!(
  object_type: "contacts",
  crm_ids: ["101", "102", "103"]
)
```

> **Rate limiting:** HubSpot enforces rate limits per private app. Batch operations process up to 100 records per request (vs 1 for single-record calls), significantly reducing the number of API calls.

---

## Airtable adapter (API v0)

Etlify ships with `Etlify::Adapters::AirtableV0Adapter`. It uses `Net::HTTP` (no external dependency) and supports both single-record and batch operations.

### Configuration

```ruby
Etlify.configure do |config|
  Etlify::CRM.register(
    :airtable,
    adapter: Etlify::Adapters::AirtableV0Adapter.new(
      access_token: ENV["AIRTABLE_TOKEN"],
      base_id: ENV["AIRTABLE_BASE_ID"]
    ),
    options: {
      rate_limit: { max_requests: 5, period: 1 }, # seconds
    }
  )
end
```

### Behaviour

- `object_type`: the Airtable table ID or name (e.g. `"tblContacts"`, `"Contacts"`).
- `id_property`: field name used to search for existing records via `filterByFormula`. If a match is found, the record is updated; otherwise a new record is created.
- `crm_id`: if provided (e.g. `"recXXXXXXXX"`), the adapter skips the search and updates the record directly.

### Example: Contact upsert

```ruby
class User < ApplicationRecord
  include Etlify::Model

  airtable_etlified_with(
    serializer: UserSerializer,
    crm_object_type: "tblContacts",
    id_property: :Email,
    sync_if: ->(user) { user.email.present? }
  )
end

# Later
user.airtable_crm_sync!
```

### Batch operations

The Airtable adapter provides two additional methods for bulk operations, using Airtable's native batch endpoints (up to 10 records per request, auto-sliced):

```ruby
adapter = Etlify::CRM.registry[:airtable].adapter

# Batch upsert via Airtable's native performUpsert
# Returns a Hash { id_property_value => record_id }
adapter.batch_upsert!(
  object_type: "tblContacts",
  records: [
    {Email: "a@example.com", Name: "Alice"},
    {Email: "b@example.com", Name: "Bob"},
  ],
  id_property: "Email"
)

# Batch delete
adapter.batch_delete!(
  object_type: "tblContacts",
  crm_ids: ["recAAA", "recBBB", "recCCC"]
)
```

> **Rate limiting:** Airtable enforces 5 requests/second/base. Batch operations process up to 10 records per request (vs 1 for single-record calls), increasing effective throughput to 50 records/second.

---

## Writing your own adapter

Implement the following interface:

```ruby
module Etlify
  module Adapters
    class MyCrmAdapter
      # Must return the remote CRM ID as a String
      def upsert!(object_type:, payload:, id_property: nil, crm_id: nil)
        # Call your CRM API to create or update
        # Return the CRM id (e.g. "12345")
      end

      # Must return true/false
      def delete!(object_type:, crm_id:)
        # Call your CRM API to delete the record
        # Return true when the remote says it has been removed
      end
    end
  end
end
```

> Keep your adapter stateless and pure. Pass all needed options explicitly and let your initializer construct it with credentials.

---

## Best practices · FAQ · Troubleshooting

### General tips

- **Start small**: sync only the fields you truly need in your serializer. You can add more later.
- **Stable payloads**: avoid non-deterministic fields (timestamps, random IDs) in the payload; they defeat idempotence.
- **Guard with `sync_if`**: skip incomplete records (e.g. no email) to reduce noise.
- **Queue selection**: route `SyncJob` to a dedicated low-priority queue to keep UX jobs snappy.

### Common questions

- **Nothing seems to happen when I call `crm_sync!`**
  Ensure you ran the migration generator and migrated the database. Also verify your `sync_if` predicate returns `true` and the serializer returns a Hash.

- **My payload keeps re-syncing even when nothing changed**
  Confirm your serializer output is stable and keys are consistently ordered/typed. If you add transient data, the digest will change on every run.

- **How do I force a refresh?**
  Change the payload (or clear the stored digest for that record) and run `crm_sync!` again. You can also add a temporary flag inside your serializer if needed.

- **Where is the CRM ID stored?**
  Etlify maintains sync state (last digest and remote ID) in your app’s database so it can skip or delete correctly.

- **Can I batch synchronise?**
  Use `Etlify::BatchSync::StaleRecordsSyncer.call(...)`. Keep batches small and let your queue handle back-pressure.

### Debugging checklist

- Credentials present and valid (e.g. `HUBSPOT_PRIVATE_APP_TOKEN`).
- Adapter set (default is a no-op NullAdapter).
- Jobs worker running (when using async).
- Serializer returns a Hash with the expected field names.
- Database table for sync state exists and is reachable.

---

## Testing

Run the test suite:

```bash
bundle exec rspec
```

### Stubbing the adapter in specs

```ruby
# In your spec
fake_adapter = instance_double("Adapter")
allow(fake_adapter).to receive(:upsert!).and_return("crm_123")
allow(fake_adapter).to receive(:delete!).and_return(true)

# Override the registry for this CRM (ex: :hubspot)
Etlify::CRM.register(
  :hubspot,
  adapter: fake_adapter,
  options: {}
)

user = create(:user, email: "someone@example.com")

# Enqueue or perform a sync for this CRM
user.hubspot_sync!(async: false)

expect(fake_adapter).to have_received(:upsert!).with(
  object_type: "contacts",
  payload: hash_including(email: "someone@example.com"),
  id_property: anything,
  crm_id: nil
)

```

---

## Adapters included

- `Etlify::Adapters::NullAdapter` (default; no-op)
- `Etlify::Adapters::HubspotV3Adapter` (API v3, with batch support)
- `Etlify::Adapters::AirtableV0Adapter` (API v0, with batch support)

---

## Licence

**MIT** — see `LICENSE`.

---

## Maintainers & Support

This library is maintained internally. Please open an issue if you need enhancements or have questions.
