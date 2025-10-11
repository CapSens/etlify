# UNRELEASED

# V0.9.0

This version contains Breaking Changes ⚠️

- Feat: Make it possible to implement multiples CRM
- Fix: Fix Etlify::StaleRecords::Finder to handle new relations and cover new use cases
- Doc: Add an `UPGRADE-GUIDE.md` (please refer to it to upgrade to this version)

# V0.8.1

- Fix: `Etlify::StaleRecords::Finder.call` when has_many :through relations with FK on source
