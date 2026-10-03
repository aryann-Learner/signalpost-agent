# Signalpost Agent

Async scraper for Norwegian company profiles from Brreg (Enhetsregisteret) and Regnskapsregisteret, built for the Signalpost hackathon challenge.

## Challenge Overview

**Scoring Breakdown (100 points total):**
- **Coverage (35 pts):** Breadth of facts extracted per company
- **Precision & Evidence (30 pts):** Correct company match + source URL traceability
- **Update Handling (20 pts):** Track changes, diff old vs. new
- **Explanations (10 pts):** Plain-language summary of what's known/missing
- **Ease of Use (5 pts):** Clean CLI, clear README

**Hard-Fail Rules:**
- One wrong-company publication = disqualification
- One fabricated financial value = disqualification
- Missing company envelope = points lost

**Resource Limits (Official Test):**
- Time: 45 minutes
- Requests: 2,000 max
- External API cost: $0 (no LLM calls)

**Submission Deliverable:**
- At least 1,000 pre-generated company profiles (JSONL)
- Repo link + pinned commit hash
- Single command to run
- README with cost/time estimates

## Features

✅ **Dynamic input:** JSON, CSV, or plain text company numbers (9-digit org numbers, never hardcoded).  
✅ **Complete profiles:** entity data, financials, board roles, and more.  
✅ **Traceability:** every fact stores source URL, retrieval timestamp, and reporting period.  
✅ **Budget management:** respects 2,000 request cap + 45-minute deadline (official mode) or auto-extends (batch mode).  
✅ **Capacity auto-tuning:** concurrency adjusts based on throttle rate; time extends only for batch mode with >1,500 companies.  
✅ **Change tracking:** SQLite versioning + diff (what changed since last run).  
✅ **No fake data:** missing fields tracked in `gaps`, never defaulted.  
✅ **Explanations:** template-based summaries (no LLM, $0 cost).  
✅ **Chunked processing:** handles 1,500+ companies in batch mode.  

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Official Mode (45-min, 2,000-request hard cap)

```bash
python signalpost.py <input_file>
```

### Batch Mode (pre-generate 1,000+ submission profiles; time extends if needed)

```bash
python signalpost.py <input_file> --mode batch
```

### Optional Arguments

```bash
python signalpost.py <input_file> \
  --mode [official|batch]        # Default: official
  --out output.jsonl             # Default: output.jsonl
  --db profiles.sqlite           # Default: profiles.sqlite
  --max-time 360                 # Max extension (minutes, batch mode only)
```

### Input Formats

- **JSON:** `["123456789", "987654321"]` or `{"companies": [...]}`
- **CSV:** `123456789,987654321`
- **Plain text:** one number per line

### Output

**output.jsonl** – one profile per line:

```json
{
  "orgnr": "123456789",
  "status": "ok",
  "retrieved_at": "2026-10-03T12:34:56Z",
  "changed_since_last_run": false,
  "diff": {"employees": {"old": 10, "new": 15}},
  "summary": "AS Example Corp registered 2015, 25 employees, headquartered in Oslo. Revenue NOK 5.2M (2024). Board: John Doe (Chair), Jane Smith. Missing: auditor, website.",
  "facts": {
    "name": {"value": "Example Corp AS", "source_url": "https://data.brreg.no/...", "retrieved_at": "..."},
    "employees": {"value": 15, "source_url": "...", "retrieved_at": "..."},
    "revenue": {"value": 5200000, "source_url": "...", "reporting_period": "2024-01-01..2024-12-31"},
    ...
  },
  "gaps": ["auditor", "website", "profit_before_tax"]
}
```

**run_report.json** – summary of this execution:

```json
{
  "input_file": "companies.txt",
  "total_companies": 1500,
  "requests_used": 1847,
  "requests_budget": 1900,
  "execution_time_minutes": 38.2,
  "time_limit_minutes": 42,
  "time_extended": false,
  "concurrency_final": 12,
  "status_breakdown": {
    "ok": 1485,
    "error": 8,
    "not_found": 5,
    "mismatch": 2
  }
}
```

## Schema & Facts Collected

### Entity (always attempted)

- `name`, `org_form`, `registered_date`, `founded_date`, `employees`
- `industry_code`, `industry`, `address`, `municipality`, `postal_code`
- `country`, `website`, `email`, `phone`
- `bankrupt`, `under_liquidation`, `vat_registered`, `parent_orgnr`
- `sub_units_count`

### Financials (only if budget allows; skipped for sub-units)

- `revenue` (driftsinntekter / sum_operating_revenue)
- `operating_result` (driftsresultat)
- `net_result` (resultat etter skatt / net_income)
- `total_assets` (sum_total_assets)
- `equity` (egenkapital)
- `profit_before_tax` (resultat før skatt)

### Roles (only if budget allows; skipped for sub-units)

- `ceo` (daglig leder)
- `board_members` (styremedlemmer) – list
- `auditor` (revisor)

### Change Tracking

- `diff` – object showing `{field: {old: x, new: y}}` for changed fields
- `changed_since_last_run` – boolean

### Explanation

- `summary` – template-based plain-language description (no LLM)

## How the Capacity Controller Works

**Rule-based, zero-cost, deterministic (no LLM):**

1. **Concurrency auto-tuning:**
   - Starts at 8 parallel workers
   - If throttle rate > 5%, halve concurrency (minimum 2)
   - If throttle rate < 1%, increase by 2 (max 24)
   - Re-evaluated every ~100 companies

2. **Time management:**
   - **Official mode:** Fixed 42-minute deadline, no extension
   - **Batch mode:** Starts at 42 minutes
     - If input > 1,500 companies AND ETA > deadline, extend to fit
     - Capped at `--max-time` (default 360 minutes = 6 hours)
     - Only companies, only for batch

3. **Work prioritization (3 phases):**
   - Phase 1: Core facts (entity data) – required
   - Phase 2: Financials – skipped if budget < 30% remaining
   - Phase 3: Roles – skipped if budget < 10% remaining
   - Every company still gets an envelope + gaps list

## Modes Explained

| Mode | Time Limit | Request Budget | Use Case |
|------|-----------|------------------|----------|
| `official` | 42 min (fixed) | 1,900 | Daily evaluation run (100 companies) |
| `batch` | 42 min → extends | 1,900 | Pre-generate 1,000+ profiles for submission |

## Known Limitations & Next Steps

### To Verify Against Real Data

1. **Financial field names** – the JSON keys in Brreg's accounts API response may differ:
   - Check `resultatregnskapResultat.driftsinntekter` for revenue
   - Check `resultatregnskapResultat.aarsresultat` for net result
   - Run 5–10 real company numbers and compare

2. **Roles endpoint structure** – verify the `/enheter/{orgnr}/roller` response format

3. **Output schema** – if the official challenge provides a canonical format, adjust the `fact()` structure and summaries

### Performance Tuning

- If 100-company official runs consistently finish early, increase concurrency start (line ~180)
- If you see frequent 429s, lower `CONCURRENCY_MAX` (line ~175)
- If batch mode runs exceed time limits, pre-filter input to remove dormant/newly-registered companies (higher miss rate, lower cost)

## Scoring Strategy

**To win the 35 coverage points:**
- Fetch roles (board, CEO, auditor) → +3–5 points
- Include financial lines beyond revenue (assets, equity, profit) → +3–5 points
- Capture registration dates, industry, parent/sub-unit links → +5–10 points
- Extract every non-null field from the entity record → +5 points
- Test with real data and fix field-mapping bugs → +5 points

**To protect the 30 precision points:**
- Match org number in every response (hard fail if mismatch)
- Store exact endpoint URL for every fact (not generic homepage links)
- Never substitute missing fields; list them in gaps
- Test the wrong-company guard with intentional mismatches

**To win the 20 update points:**
- SQLite stores historical versions (✓ implemented)
- Diff highlights what changed (✓ implemented)
- Re-run against a company you already fetched; verify `changed_since_last_run` and diff are correct

**To win the 10 explanation points:**
- Summaries use templates, not LLM (✓ implemented, $0 cost)
- They state what is known, what is missing, and key facts

## Submission Checklist

- [ ] Repo pushed with working signalpost.py + requirements.txt + README
- [ ] Generate 1,000+ company profiles: `python signalpost.py companies_1000.txt --mode batch`
- [ ] Commit the output JSONL and run_report.json
- [ ] Test official mode on 10 random companies in under 2 minutes: `python signalpost.py test_10.txt`
- [ ] Verify one company's data against Brreg web UI (spot-check name, industry, revenue)
- [ ] Finalize README with:
  - Exact commit hash for submission
  - Estimated run cost: $0 (no LLM)
  - Estimated time for 100 companies: ~8–12 minutes
  - Estimated time for 1,000 companies (batch): ~50–90 minutes
- [ ] Copy repo link and commit hash to submission form

## Deadline

**Code revisions accepted until:** 18 Oct, 11:59 PM IST  
**Days remaining:** ~14 days (as of 17 Oct)

## Questions?

Run a test against a real company number and share the output. I can debug any field-mapping errors.
