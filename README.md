# Etlify

> Rails-first, idempotent synchronisation between your ActiveRecord models and your CRM. HubSpot is supported out of the box; other CRMs can be plugged in via adapters.

This gem has been designed by [Capsens, a fintech web and mobile agency based in Paris](https://capsens.eu/).

---

## Why Etlify? (Context & Intended Use)

In internal products, it is common to persist domain data in Rails while also mirroring a subset of it into a CRM for marketing, sales or support workflows. Etlify provides a small, dependable toolkit to **declare** which models are CRM-backed, **serialise** them into CRM payloads, and **synchronise** them in an **idempotent** fashion so repeated calls are safe and efficient.

Etlify sits beside your app; it does **not** try to own your domain or background processing. It integrates naturally with ActiveRecord and ActiveJob so you keep your current architecture and simply “switch on” CRM sync where you need it.

---

## Features at a glance

| Area          | What you get                                                  | Why it helps                                        |
| ------------- | ------------------------------------------------------------- | --------------------------------------------------- |
| DSL           | `include Etlify::Model` + `etlified_with(...)` on your models | Opt-in sync with a single line; clear, local intent |
| Serialisers   | A base class to turn a model into a CRM payload               | Keeps mapping logic where it belongs; easy to test  |
| Adapters      | HubSpot adapter included; plug your own                       | Swap CRMs without touching model code               |
| Idempotence   | Stable digest of the last synced payload                      | Avoids redundant API calls; safe to retry           |
| Jobs          | `crm_sync!` enqueues an ActiveJob (`SyncJob`) or runs inline  | Fits your queue; simple to trigger                  |
| Delete        | `crm_delete!` to remove a record from the CRM                 | Keeps both sides consistent                         |
| Configuration | Logger, queue name, digest strategy, adapter                  | Control behaviour without scattering settings       |

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
    options: {job_class: "Etlify::SyncObjectWorker"}
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
  @logger = Rails.logger
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
    dependencies: [:investments]
  )
end
```

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
# Enqueue (default): one job per stale record
Etlify::StaleRecords::BatchSync.call

# Restrict to specific models
Etlify::StaleRecords::BatchSync.call(models: [User, Company])

# Restrict to specifics CRM
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
  per_model: { "User" => 42, ...}, # per-model breakdown
  errors:    Integer               # number of errors in async:false mode
}
```

> By default, jobs are enqueued via `"Etlify::SyncJob"` and executed by your
> ActiveJob backend. It can be overriden per CRM when registering it
> It is very usefull to handle custom throttling rules

```
Etlify::CRM.register(
  :hubspot,
  adapter: Etlify::Adapters::HubspotV3Adapter.new(
    access_token: ENV["HUBSPOT_PRIVATE_APP_TOKEN"]
  ),
  options: {job_class: "Etlify::SyncObjectWorker"}
)

> the chosen class must implement .perform_later(record_class, id, crm_name)
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
  - in **async: true** mode (default): **enqueue** one job per ID without loading
    full records into memory;
  - in **async: false** mode: load each record and pass it to
    `Etlify::Synchronizer.call(record)` **inline**
    (errors are logged and counted without interrupting the batch).

## Best practices

- **Production**: prefer `async: true` with a **dedicated, low-priority queue**
  via ActiveJob.
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
- **Observe logs**: Etlify uses your configured logger; in development, check the console.
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
- `Etlify::Adapters::HubspotV3Adapter` (API v3)

---

## Licence

**MIT** — see `LICENSE`.

---

## Maintainers & Support

This library is maintained internally. Please open an issue if you need enhancements or have questions.
