---
name: contracts
description: Project-wide invariants every change to data-seb must respect
type: contract
---

# Contracts

## Coding habits (mandatory)

1. **Think before coding.** State the goal, the approach, and the files-to-touch list before writing any code.
2. **Simplicity first.** Prefer the smallest change that solves the stated problem. No speculative abstraction.
3. **Surgical edits only.** Modify the specific lines required; leave surrounding code untouched. No drive-by refactors.
4. **Goal-directed objectives stated up front.** Before each task, write what "done" looks like and what the verification step is.

## Project invariants

- **Version bumps live in `pyproject.toml`.** Any user-visible behavior change bumps the `version` field there in the same commit.
- **Generated artifacts do not get committed.** Output XLSX/CSV files belong in `artifacts/` (gitignored), not at the repo root.
- **No network calls in import paths.** Fetchers in `data_seb/*.py` perform I/O inside functions, not at module top level.
- **Public API is what `data_seb/__init__.py` re-exports.** Adding/removing a re-export is a breaking change and must bump the minor version.
