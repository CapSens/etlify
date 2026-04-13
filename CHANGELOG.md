# UNRELEASED

# V0.10.0

- Feat: Extract `DefaultHttp` into a shared class (`lib/etlify/adapters/default_http.rb`) for reuse across adapters.
- Feat: Add `batch_upsert!` and `batch_delete!` to `HubspotV3Adapter`. Leverages HubSpot's native batch endpoints (`POST /batch/upsert` and `POST /batch/archive`, up to 100 inputs per request). `batch_upsert!` returns a `Hash{id_property_value => crm_id}` for reliable mapping.
- Feat: Add `BatchSyncJob` — a single job per CRM that processes all stale records instead of enqueuing one `SyncJob` per record. Includes built-in rate limiting via a new `rate_limit` option on `Etlify::CRM.register`. The rate limiter is injected at the adapter level (per HTTP request). On `RateLimited` (429), the job re-enqueues with remaining records after backoff. `StaleRecords::BatchSync` now enqueues one `BatchSyncJob` per CRM in async mode.
- Feat: Add `Etlify::RateLimiter` — sleep-based rate limiter with configurable `max_requests` / `period`.
- Feat: Add `Etlify::BatchSynchronizer` — batch-aware synchronizer that applies per-record pre-checks (guard, digest, dependencies) then calls `adapter.batch_upsert!` for all ready records. Used by `BatchSyncJob` when the adapter supports it, with fallback to sequential `Synchronizer.call`.
- Feat: Adapters now support an optional `rate_limiter=` accessor for per-HTTP-request throttling.
- Feat: Add `batch_upsert!` and `batch_delete!` to `NullAdapter` for test support.
- Feat: Add `AirtableV0Adapter` for Airtable API v0 integration. Supports `upsert!` and `delete!` (standard Etlify interface) plus batch operations: `batch_upsert!` (via Airtable's native `performUpsert`, up to 10 records per request) and `batch_delete!`. Uses `Net::HTTP` (zero external dependency), injectable `http_client:` for testing, and structured error handling via the Etlify error hierarchy. Supports rate limiting via `rate_limiter=` accessor.

# V0.9.4

- Feat: Add `stale_scope` option to CRM DSL to restrict which records the `StaleRecords::Finder` considers. Accepts a lambda returning an ActiveRecord scope, applied at SQL level before any record is processed. This prevents unnecessary `CrmSynchronisation` rows for records that `sync_if` would skip. Models that do not specify `stale_scope` are not affected — the Finder behaves exactly as before.
- Feat: Add `error_count` column to `crm_synchronisations` to track consecutive sync failures. Records exceeding the configurable `max_sync_errors` limit (default: 3) are automatically excluded from `StaleRecords::Finder`. The limit can be set globally via `config.max_sync_errors` or per CRM via `options: { max_sync_errors: N }`. Use `CrmSynchronisation#reset_error_count!` to manually re-enable sync after fixing the root cause. Run `rails g etlify:add_error_count` to generate the migration.
- Feat: Add `sync_dependencies` option for dependency-based sync ordering. When a dependency has no `crm_id` yet, the sync is buffered in `etlify_pending_syncs` and automatically retried once the dependency is synced. Supports both etlified models (via `CrmSynchronisation`) and legacy models with a direct `#{crm_name}_id` column (e.g. `airtable_id`). Requires running `rails g etlify:migration create_etlify_pending_syncs && rails db:migrate`.
- Fix: `StaleRecords::Finder` now correctly handles `has_one :through` dependencies where the through association is a `belongs_to` (FK on owner table instead of through table). Also adds polymorphic `source_type` filtering on the JOIN when the source is polymorphic.
- Fix: Handle STI subclasses in `StaleRecords::Finder` to avoid `PG::UndefinedColumn` errors. Uses `base_class.unscoped` in `stale_relation_for` and adds the STI type filter manually on the inner query, preventing Rails from injecting `WHERE type = '...'` on a subquery alias that doesn't expose the `type` column. Also filters out STI subclasses that only inherited `etlify_crms` via `class_attribute` in `etlified_models`.

# V0.9.3

- Fix: Support custom `job_class` in `BatchSync` via CRM options

# V0.9.2

- fix: Third level depencies errors in `Etlify::StaleRecords::Finder`

# V0.9.1

- fix: Avoid aliases in `Etlify::StaleRecords::Finder` to make it possible to use .pluck(:id) on collection

# V0.9.0

This version contains Breaking Changes ⚠️

- Feat: Make it possible to implement multiples CRM
- Fix: Fix Etlify::StaleRecords::Finder to handle new relations and cover new use cases
- Doc: Add an `UPGRADE-GUIDE.md` (please refer to it to upgrade to this version)

# V0.8.1

- Fix: `Etlify::StaleRecords::Finder.call` when has_many :through relations with FK on source
