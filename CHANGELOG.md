# UNRELEASED

- Feat: Add `stale_scope` option to CRM DSL to restrict which records the `StaleRecords::Finder` considers. Accepts a lambda returning an ActiveRecord scope, applied at SQL level before any record is processed. This prevents unnecessary `CrmSynchronisation` rows for records that `sync_if` would skip. Models that do not specify `stale_scope` are not affected — the Finder behaves exactly as before.
- Feat: Add `sync_dependencies` option for dependency-based sync ordering. When a dependency has no `crm_id` yet, the sync is buffered in `etlify_pending_syncs` and automatically retried once the dependency is synced. Requires running `rails g etlify:migration create_etlify_pending_syncs && rails db:migrate`.


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
