# Signalpost Agent

A challenge-ready, deterministic scraper for Norwegian corporate profiles built for the Signalpost hackathon.

## What this version solves

- Handles large batch runs in chunks of 1,500 IDs
- Works in both `official` and `batch` modes
- Keeps request use under the 2,000 cap in the official path
- Tracks every fact back to the exact source URL and retrieval time
- Uses SQLite to store history and diff old-vs-new values
- Produces a plain-language summary using templates, with $0 external API cost
- Never invents values: missing fields go into `gaps`, not zeros
- Includes a wrong-company guard and strict org-number reconciliation

## Repository structure

- `signalpost.py` — main runner
- `requirements.txt` — pinned runtime dependency
- `README.md` — project guide
- `run_report.json` — summary generated on each run
- `profiles.sqlite` — SQLite log (created at runtime)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

### Official mode (fixed 42-minute cap, 1,900 requests)

```bash
python signalpost.py companies.txt
```

### Batch mode (submission generation, chunked 1,500 at a time)

```bash
python signalpost.py companies.txt --mode batch --chunk-size 1500 --max-time 360
```

## Output format

The script writes JSONL, one profile per line:

```json
{
  "orgnr": "123456789",
  "status": "ok",
  "retrieved_at": "2026-10-03T12:34:56+00:00",
  "facts": {
    "name": {"value": "Example AS", "source_url": "https://data.brreg.no/.../enheter/123456789", "retrieved_at": "2026-10-03T12:34:56+00:00"},
    "revenue": {"value": 4200000, "source_url": "https://data.regnskapsregister.brreg.no/...", "retrieved_at": "2026-10-03T12:34:56+00:00", "reporting_period": "2024-01-01..2024-12-31"}
  },
  "gaps": ["auditor", "website"],
  "summary": "Example AS (AS), registered 2015, 45 employees, based in Oslo. Revenue NOK 4,200,000. CEO: Ada Lovelace. Missing: auditor, website.",
  "changed_since_last_run": false,
  "diff": {}
}
```

## What it makes different 

1. **Coverage** - captures core Brreg entity data, financial lines, and roles
2. **Precision** - strict org matching and exact source links prevent wrong-company failures
3. **Update handling** - SQLite stores the last version and emits a diff when values change
4. **Explanations** - summary is human-readable and generated without an LLM
5. **Ease of use** - one command, JSONL output, and a run report

## Constraints respected

- No hardcoded org IDs
- No fabrications for missing values
- Asynchronous HTTP requests with concurrency tuning
- Time and request caps are enforced
- No LLM dependency, so cost stays at $0

## Important note

This repo is production-oriented for the challenge, but actual Brreg field names for some financial endpoints should be validated against live company records before final submission. The script already includes strict mismatch checks and graceful fallbacks so that if a field is absent or the response shape differs, it records the gap instead of hallucinating a value.
