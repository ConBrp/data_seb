---
name: data-seb
description: Entry file for agents working on the data-seb Python package — fetchers for BCRA/INDEC economic data
type: contract
---

# data-seb

Python library for fetching and processing economic data from the BCRA (Central
Bank of Argentina) and INDEC: monetary aggregates, exchange rates, inflation
indicators. Source in `data_seb/`, package metadata in `pyproject.toml`, CLI
entry point `export-bcra` → `data_seb.tools.exporter:main`.

## Pointers

- `INDEX.md` — module-level retrieval hooks (which file fetches what).
- `CONTRACTS.md` — invariants the agent must respect on every change.
- `pyproject.toml` — dependencies, version, scripts.

## Coding habits (project default)

1. **Think before coding.** State the goal, the approach, and the files-to-touch list before writing any code.
2. **Simplicity first.** Prefer the smallest change that solves the stated problem. No speculative abstraction.
3. **Surgical edits only.** Modify the specific lines required; leave surrounding code untouched. No drive-by refactors.
4. **Goal-directed objectives stated up front.** Before each task, write what "done" looks like and what the verification step is.
