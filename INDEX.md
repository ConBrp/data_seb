# INDEX

Module-level retrieval hooks. Open the linked file when the question matches the hook.

## Root

- [AGENTS.md](AGENTS.md) — project brief, pointers, four coding habits.
- [CONTRACTS.md](CONTRACTS.md) — invariants every change must respect.
- [pyproject.toml](pyproject.toml) — version, dependencies, `export-bcra` script entry.

## Package (`data_seb/`)

- [bcra.py](data_seb/bcra.py) — BCRA fetchers: principales variables, TCRM, com3500, balance, REM, TAMAR; scrapes BCRA Estadísticas pages.
- [ipc.py](data_seb/ipc.py) — INDEC IPC: divisiones, aperturas, ponderadores; raw inflation series.
- [ipc_se.py](data_seb/ipc_se.py) — Seasonally adjusted IPC via X-13ARIMA-SEATS with STL fallback (uses `ipc.py`).
- [cpi.py](data_seb/cpi.py) — US BLS CPI fetcher (registration-key API).
- [dolar.py](data_seb/dolar.py) — USD/ARS quotes from bluelytics evolution endpoint.
- [pbi.py](data_seb/pbi.py) — INDEC EMAE (monthly activity) supply-and-demand sheets.
- [cod.py](data_seb/cod.py) — Date-coding helper (`Date_Cod` mm-yyyy column) reused across fetchers.
- [tools/exporter.py](data_seb/tools/exporter.py) — `export-bcra` CLI: dumps BCRA variables to XLSX.
