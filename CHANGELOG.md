# UNRELEASED

- Fix: `Etlify::StaleRecords::BatchSync#enqueue_batch_jobs` now respects the `batch_size` option in async mode. Previously, all stale pairs for a CRM were flattened into a single `BatchSyncJob`, regardless of `batch_size`. With many records, the resulting HTTP fan-out (e.g. one `BatchSyncJob` doing hundreds of batch upsert calls) was prone to `Net::ReadTimeout`: a single timeout would crash the entire job and Sidekiq would retry forever with the same oversized args. Pairs are now sliced by `batch_size` and enqueued as N independent `BatchSyncJob`s — a failure is bounded to one chunk instead of the full stale population.
- Fix: `Etlify::BatchSyncJob` lock key now differentiates discovery runs from explicit chunk runs. The previous global per-CRM lock (`etlify:batch_sync_lock:<crm>`) silently dropped sibling chunk enqueues from the new sliced `BatchSync`. Discovery mode (no `record_pairs` argument) keeps a per-CRM lock under `:discovery` to prevent piling up cron-triggered runs. Chunk mode (explicit `record_pairs`) uses a per-content lock under `:chunk:<sha256(pairs)>` so independent chunks can be enqueued and executed in parallel while identical re-enqueues are still deduplicated. The `reenqueue` path now clears the current job's lock based on its actual arguments rather than the CRM-only key, so a `RateLimited` re-enqueue with overlapping pairs can succeed.

# V0.12.0

This version contains Breaking Changes ⚠️ (please refer to `UPGRADE-GUIDE.md`)

- Breaking: Replace the `id_property:` DSL option with a mandatory `match_by: {property:, value:}` option, fully decoupling **matching** (how Etlify finds the CRM record) from the **payload** (what the serializer syncs). `property` is the unique CRM property used to match; `value` resolves the matching value from the record (method name Symbol or Proc). The serializer payload is now synced **as-is** on every path: the matching property is only written at creation time (when the payload does not already carry it) and is **never** written on an existing record unless the serializer explicitly includes it. This fixes a HubSpot bug where a contact's **primary email was overwritten** by the platform email when the contact had been matched through one of its secondary emails (`hs_additional_emails`): the upsert payload carried `email`, and writing `email` on a contact promotes the value to primary. Leaving `email` out of the serializer now guarantees the sync never touches the contact's emails (verified against the HubSpot API: `/batch/upsert` matches secondary emails, and creation sets the email from the input `id` even when absent from `properties`).
- Breaking: Adapter interface change. `upsert!` now receives `match_property:`/`match_value:` instead of `id_property:` (the value no longer travels inside the payload, and is no longer extracted from it). `batch_upsert!` now receives `inputs:` (an Array of `{value:, properties:}`) instead of `records:`/`id_property:`, and returns a Hash mapping each input's value **as provided** to the CRM id (lookups no longer break when the CRM normalizes values, e.g. HubSpot lowercasing emails). Applies to `HubspotV3Adapter`, `AirtableV0Adapter` and `NullAdapter`; custom adapters must be updated (see `README.md` "Writing your own adapter").
- Breaking: A record with **no** `crm_id` whose `match_by` value resolves blank now fails explicitly (`:error`, `error_count` bumped) instead of creating an unmatched CRM record. A blank value with a known `crm_id` is fine (update by id). In `BatchSynchronizer`, a raising `match_by` proc or a blank value is isolated per record and never blocks the rest of the batch.
- Breaking: `NullAdapter` now enforces the same contract as the real adapters — `upsert!` raises `ArgumentError` when `match_value` is blank and no `crm_id` is known, and `batch_upsert!` raises on any blank input value, instead of generating a random UUID. Dev/test environments now surface the same `:error` results as production. Update test fixtures that relied on fake-syncing records with a blank matching value.
- Fix: `BatchSynchronizer` no longer marks a record as synced when the mapping returned by `batch_upsert!` does not contain its match value (e.g. the CRM response does not echo the match property back). Previously the row was updated with `crm_id: nil` **and** a fresh `last_digest`, silently sticking the record forever (subsequent syncs hit `:not_modified`). It now fails explicitly (`last_error` + `error_count` bump), stays stale, and is retried until fixed or excluded via `max_sync_errors`.
- Note (Airtable): `performUpsert` has no separate id slot, so the match field is injected into `fields` when the payload does not carry it. This is safe: Airtable matches on strict field equality, so a matched record already holds the injected value (no-op write). When the payload **does** carry the match field, the payload wins — its value should equal `match_by`'s, otherwise the returned mapping cannot reference the input value and the record fails explicitly (`:error` + `error_count` bump) on first sync until the values are aligned.

# V0.11.3

- Fix: Records that already have a `crm_id` are now synced **by `crm_id`** (direct update) instead of being re-resolved through `id_property`. `id_property` is used **only for the first reconciliation** (to avoid creating a duplicate when the record may already exist on the CRM). This removes unique-property collisions (e.g. HubSpot's unique `cs_identifier`) that occurred when the `id_property` value (e.g. `email`) no longer matched the existing CRM record: the adapter used to search by `email`, miss, and try to *create* a new object holding an already-taken unique value (HTTP 400 `VALIDATION_ERROR`). Applies to both `Etlify::Synchronizer` (individual) and `Etlify::BatchSynchronizer` (batch). The batch path now partitions ready records into "already has a `crm_id`" (new `adapter.batch_update!`, native `/batch/update` on HubSpot, PATCH-by-record-id on Airtable) and "first sync" (existing `batch_upsert!` by `id_property`). A `crm_id` pointing to a deleted CRM record raises `Etlify::NotFound` and bumps `error_count` (no silent re-create) — fix manually then call `CrmSynchronisation#reset_error_count!`.
- Fix: `BatchSynchronizer` now falls back to the per-record sequential loop on any deterministic per-record CRM error — `Etlify::ValidationFailed` **and** other 4xx `Etlify::ApiError` (e.g. HubSpot's `400` on a unique-property collision) — not only `ValidationFailed`. Previously the `400` (an `ApiError`) escaped the `ValidationFailed`-only fallback and crashed the whole `BatchSyncJob` without ever bumping `error_count`, so the Finder never excluded the offender and the batch retried the same failure forever. `RateLimited` (429) and `Unauthorized` (401/403), as well as 5xx/transport errors, still bubble up (re-enqueue with backoff / job retry) and never bump `error_count`. The fallback is scoped per group so a failure in one partition never re-processes the other.
- Feat: Add `batch_update!(object_type:, records:)` to `HubspotV3Adapter`, `AirtableV0Adapter` and `NullAdapter`. Updates each object by its known CRM id (`{crm_id:, properties:}` per record) and returns an identity `Hash{crm_id => crm_id}` mapping. HubSpot uses the native `POST /crm/v3/objects/{type}/batch/update` (up to 100 inputs); Airtable uses `PATCH` with explicit record ids (up to 10).

# V0.11.2

- Fix: `BatchSynchronizer#perform_batch_upsert!` now falls back to a per-record `adapter.upsert!` loop when `batch_upsert!` raises `Etlify::ValidationFailed`. Batch endpoints (e.g. Airtable's `performUpsert`) are atomic: a single bad record (duplicate on merge field, dead reference, etc.) makes the whole batch return 422 and, before this fix, the per-record loop where `error_count` is bumped was never reached. Sidekiq retried the whole batch with the same args indefinitely, blocking all healthy records sharing the batch. With the fallback, healthy records are synced sequentially and only the offending record gets its `error_count` incremented — after `max_sync_errors` failures, the Finder excludes it and the batch is unblocked. `Etlify::RateLimited` is explicitly re-raised in both the batch path and the per-record fallback loop, so it always bubbles up to `BatchSyncJob` which can re-enqueue with backoff (and is not mis-classified as a per-record error that would bump `error_count`).

# V0.11.1

- Fix: `AirtableV0Adapter#batch_upsert!` now passes `returnFieldsByFieldId: true` in the `performUpsert` request when `id_property` is an Airtable field ID (e.g. `"fldXXXXXXXXXXXXXX"`). Without this flag, Airtable returned the response fields keyed by name, while `extract_batch_mapping` looked them up by ID, leading to an empty mapping. The records were still created/updated on Airtable, but `BatchSynchronizer` then wrote `crm_id: nil` (with `last_digest` set) on the `crm_synchronisations` row, leaving it stuck (subsequent syncs hit `:not_modified` due to digest match). The flag is added conditionally based on the `fld` prefix to preserve backward compatibility with field-name usage.

# V0.11.0

- Feat: Add `enabled:` flag to `Etlify::CRM.register` (default `true`). When a CRM is registered with `enabled: false`, all sync and delete calls become a no-op: `Model#crm_sync!` and `Model#crm_delete!` return `true` without enqueuing any job, `Etlify::Synchronizer.call` and `Etlify::Deleter.call` return `:disabled`, `Etlify::BatchSynchronizer.call` returns stats with `disabled: true`, and `Etlify::StaleRecords::BatchSync.call` silently skips disabled CRMs while still processing enabled ones. No adapter call, no write to `crm_synchronisations`. Useful to keep Etlify dormant in development or test environments. New public helper `Etlify::CRM.enabled?(name)` (returns `true` for unknown CRMs as a safe default).

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
