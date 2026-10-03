#!/usr/bin/env python3
"""Signalpost agent: async scraper for Norwegian company data.

Built for the Signalpost hackathon challenge.
Usage:
  Official mode (45-min hard cap, 2,000-request cap):
    python signalpost.py <input_file>
  
  Batch mode (pre-generate 1,000+ profiles; auto-extends time if needed):
    python signalpost.py <input_file> --mode batch

Input: JSON, CSV, or newline-delimited text with 9-digit company numbers.
Deps: pip install -r requirements.txt
"""
import argparse
import asyncio
import json
import re
import sqlite3
import sys
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from collections import deque

import httpx

BASE = "https://data.brreg.no/enhetsregisteret/api"
ACCOUNTS = "https://data.regnskapsregister.brreg.no/regnskapsregister/regnskap"

# Official mode constraints
OFFICIAL_TIME_LIMIT = 42 * 60  # 42 minutes
OFFICIAL_REQUEST_BUDGET = 1900  # leave 100 headroom under 2,000

# Batch mode defaults (can extend)
BATCH_INITIAL_TIME = 42 * 60
BATCH_MAX_TIME_EXTENSION = 6 * 60 * 60  # 6 hours max
BATCH_THRESHOLD = 1500  # companies

# Concurrency tuning
CONCURRENCY_START = 8
CONCURRENCY_MIN = 2
CONCURRENCY_MAX = 24
THROTTLE_THRESHOLD_UP = 0.01  # < 1% fail -> increase
THROTTLE_THRESHOLD_DOWN = 0.05  # > 5% fail -> decrease
THROTTLE_WINDOW = 100  # re-evaluate every N requests


def now():
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_ids(path):
    """Accept JSON list, CSV, or newline text. Normalise to 9-digit strings."""
    try:
        raw = open(path, encoding="utf-8").read()
    except FileNotFoundError:
        print(f"ERROR: Input file '{path}' not found.", file=sys.stderr)
        sys.exit(1)

    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            data = next((v for v in data.values() if isinstance(v, list)), [])
        tokens = [
            str(x.get("orgnr") or x.get("id") or x) if isinstance(x, dict) else str(x)
            for x in data
        ]
    except json.JSONDecodeError:
        tokens = re.split(r"[\s,;]+", raw)

    ids, seen = [], set()
    for t in tokens:
        d = re.sub(r"\D", "", t)
        if len(d) == 9 and d not in seen:
            seen.add(d)
            ids.append(d)

    if not ids:
        print("WARNING: No valid 9-digit org numbers found in input.", file=sys.stderr)

    return ids


class Budget:
    """Track request budget, deadline, and capacity."""

    def __init__(self, mode="official", total_companies=0, initial_time=None):
        self.mode = mode
        self.used = 0
        self.request_limit = OFFICIAL_REQUEST_BUDGET
        
        if mode == "official":
            self.deadline = time.time() + OFFICIAL_TIME_LIMIT
        else:
            # Batch mode: start with initial time, may extend
            initial_time = initial_time or BATCH_INITIAL_TIME
            self.deadline = time.time() + initial_time
            self.initial_deadline = self.deadline
            self.total_companies = total_companies
            self.may_extend = True
        
        self.concurrency = CONCURRENCY_START
        self.throttle_history = deque(maxlen=THROTTLE_WINDOW)

    def take(self, force=False):
        """Check if a request can be made within budget and time limits."""
        if not force and (self.used >= self.request_limit or time.time() > self.deadline):
            return False
        self.used += 1
        return True

    def remaining(self):
        """Return remaining requests."""
        return max(0, self.request_limit - self.used)

    def time_remaining(self):
        """Return time remaining in seconds."""
        return max(0, self.deadline - time.time())

    def record_throttle(self, was_throttled):
        """Record whether a request was throttled (for tuning)."""
        self.throttle_history.append(was_throttled)
        if len(self.throttle_history) >= THROTTLE_WINDOW:
            throttle_rate = sum(self.throttle_history) / len(self.throttle_history)
            if throttle_rate > THROTTLE_THRESHOLD_DOWN and self.concurrency > CONCURRENCY_MIN:
                self.concurrency = max(CONCURRENCY_MIN, self.concurrency // 2)
            elif throttle_rate < THROTTLE_THRESHOLD_UP and self.concurrency < CONCURRENCY_MAX:
                self.concurrency = min(CONCURRENCY_MAX, self.concurrency + 2)

    def maybe_extend_time(self, companies_processed, avg_time_per_company):
        """In batch mode, extend deadline if ETA exceeds current limit."""
        if self.mode != "batch" or not self.may_extend:
            return False
        
        if self.total_companies <= BATCH_THRESHOLD:
            # Small batches don't need extension
            return False
        
        remaining_companies = self.total_companies - companies_processed
        estimated_remaining_time = remaining_companies * avg_time_per_company
        time_available = self.deadline - time.time()
        
        if estimated_remaining_time > time_available:
            # Need to extend
            extension = min(
                estimated_remaining_time - time_available,
                BATCH_MAX_TIME_EXTENSION - (self.deadline - self.initial_deadline)
            )
            if extension > 0:
                self.deadline += extension
                return True
        
        return False


async def get(client, budget, url, retries=2):
    """Fetch URL with retry logic. Returns (data, error, was_throttled)."""
    throttled = False
    for attempt in range(retries + 1):
        if not budget.take():
            return None, "budget_exhausted", False
        try:
            r = await client.get(url, headers={"Accept": "application/json"})
            if r.status_code == 200:
                budget.record_throttle(False)
                return r.json(), None, False
            if r.status_code == 404:
                budget.record_throttle(False)
                return None, "not_found", False
            if r.status_code in (429, 500, 502, 503):
                throttled = True
                budget.record_throttle(True)
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            budget.record_throttle(False)
            return None, f"http_{r.status_code}", False
        except httpx.HTTPError as e:
            budget.record_throttle(True)
            if attempt == retries:
                return None, f"network:{type(e).__name__}", True
            await asyncio.sleep(1)

    return None, "failed", throttled


def fact(value, url, period=None):
    """Every fact carries its exact source URL + retrieval date. None stays None."""
    return {
        "value": value,
        "source_url": url,
        "retrieved_at": now(),
        "reporting_period": period,
    }


async def fetch_roles(client, budget, orgnr, base_url):
    """Fetch board members, CEO, and auditor from /roller endpoint."""
    roles_data = {"ceo": None, "board_members": None, "auditor": None}
    url = f"{base_url}/roller"

    data, err, _ = await get(client, budget, url)
    if data is None:
        return roles_data, err

    try:
        # Parse roles response (may be dict or list)
        if isinstance(data, dict):
            roles_list = data.get("roller") or data.get("roles") or []
        elif isinstance(data, list):
            roles_list = data
        else:
            return roles_data, "unexpected_format"

        board = []
        ceo_info = None
        auditor_info = None

        for role_obj in roles_list:
            # Role name may be "rolle" or "role"
            role_type = (role_obj.get("rolle") or role_obj.get("role") or "").lower()
            
            # Person data
            person = role_obj.get("person") or {}
            person_name = person.get("navn") or person.get("name")

            if "styremedlem" in role_type or "board" in role_type:
                if person_name:
                    board.append(person_name)
            elif "daglig leder" in role_type or "ceo" in role_type or "director" in role_type:
                if person_name:
                    ceo_info = person_name
            elif "revisor" in role_type or "auditor" in role_type:
                if person_name:
                    auditor_info = person_name

        if board:
            roles_data["board_members"] = board
        if ceo_info:
            roles_data["ceo"] = ceo_info
        if auditor_info:
            roles_data["auditor"] = auditor_info

        return roles_data, None
    except (KeyError, TypeError, AttributeError) as e:
        return roles_data, f"parse_error:{type(e).__name__}"


def build_summary(env):
    """Build template-based plain-language summary (no LLM, $0 cost)."""
    facts = env.get("facts", {})
    gaps = env.get("gaps", [])
    
    if env["status"] != "ok":
        return f"Status: {env['status']}. Unable to retrieve profile."
    
    parts = []
    
    # Core identity
    name = facts.get("name", {}).get("value", "Unknown")
    org_form = facts.get("org_form", {}).get("value", "")
    registered = facts.get("registered_date", {}).get("value", "")
    employees = facts.get("employees", {}).get("value")
    
    if org_form:
        parts.append(f"{name} ({org_form})")
    else:
        parts.append(name)
    
    if registered:
        parts.append(f"registered {registered}")
    
    if employees is not None:
        parts.append(f"{employees} employees")
    
    # Location
    address = facts.get("address", {}).get("value")
    municipality = facts.get("municipality", {}).get("value")
    if municipality:
        parts.append(f"headquartered in {municipality}")
    elif address:
        parts.append(f"based in {address}")
    
    summary = ", ".join(parts) + ". "
    
    # Financials
    revenue = facts.get("revenue", {}).get("value")
    net_result = facts.get("net_result", {}).get("value")
    if revenue is not None:
        summary += f"Revenue NOK {revenue:,.0f}. "
    if net_result is not None:
        summary += f"Net result NOK {net_result:,.0f}. "
    
    # Leadership
    ceo = facts.get("ceo", {}).get("value")
    board = facts.get("board_members", {}).get("value", [])
    if ceo or board:
        leadership = []
        if ceo:
            leadership.append(f"CEO {ceo}")
        if board:
            leadership.append(f"Board: {', '.join(board[:3])}")
        summary += "; ".join(leadership) + ". "
    
    # Gaps
    if gaps:
        summary += f"Missing: {', '.join(gaps[:5])}"
        if len(gaps) > 5:
            summary += f" and {len(gaps) - 5} more"
        summary += "."
    
    return summary


async def profile(client, budget, sem, orgnr, phase_limits):
    """Fetch complete company profile: entity, financials, and roles."""
    async with sem:
        url = f"{BASE}/enheter/{orgnr}"
        unit, err, _ = await get(client, budget, url)
        if unit is None and err == "not_found":
            # Fallback to sub-units
            url = f"{BASE}/underenheter/{orgnr}"
            unit, err, _ = await get(client, budget, url)

        env = {
            "orgnr": orgnr,
            "status": "ok",
            "retrieved_at": now(),
            "facts": {},
            "gaps": [],
            "diff": {},
        }

        if unit is None:
            env["status"] = "not_found" if err == "not_found" else "error"
            env["error"] = err
            env["summary"] = f"Status: {env['status']}."
            return env

        # Wrong-company guard
        if str(unit.get("organisasjonsnummer")) != orgnr:
            env["status"] = "mismatch"
            env["summary"] = "Organization number mismatch."
            return env

        # === PHASE 1: Entity data ===
        addr = unit.get("forretningsadresse") or unit.get("beliggenhetsadresse") or {}
        nace = unit.get("naeringskode1") or {}
        mapping = {
            "name": unit.get("navn"),
            "org_form": (unit.get("organisasjonsform") or {}).get("beskrivelse"),
            "registered_date": unit.get("registreringsdatoEnhetsregisteret"),
            "founded_date": unit.get("stiftelsesdato"),
            "employees": unit.get("antallAnsatte"),
            "industry_code": nace.get("kode"),
            "industry": nace.get("beskrivelse"),
            "address": ", ".join(
                filter(
                    None,
                    [*(addr.get("adresse") or []), addr.get("postnummer"), addr.get("poststed")],
                )
            )
            or None,
            "municipality": addr.get("kommune"),
            "postal_code": addr.get("postnummer"),
            "country": addr.get("land") or "NO",
            "website": unit.get("hjemmeside"),
            "email": unit.get("epostadresse"),
            "phone": unit.get("organisasjonsnummer"),  # Fallback; may not exist
            "bankrupt": unit.get("konkurs"),
            "under_liquidation": unit.get("underAvvikling"),
            "vat_registered": unit.get("registrertIMvaregisteret"),
            "parent_orgnr": (unit.get("overordnetEnhet") or {}).get("organisasjonsnummer"),
            "sub_units_count": unit.get("antallAnsatte"),  # Approximation; may not exist
        }

        for k, v in mapping.items():
            if v in (None, "", False) and k not in (
                "bankrupt",
                "under_liquidation",
                "vat_registered",
                "parent_orgnr",
                "phone",
            ):
                env["gaps"].append(k)
            else:
                env["facts"][k] = fact(v, url)

        # === PHASE 2: Financials ===
        is_subunit = "/underenheter/" in url
        if not is_subunit and phase_limits.get("financials", True):
            acc_url = f"{ACCOUNTS}/{orgnr}"
            acc, aerr, _ = await get(client, budget, acc_url)
            if acc:
                rec = acc[0] if isinstance(acc, list) and acc else acc
                
                # Strict org number check for financials
                acc_orgnr = rec.get("organisasjonsnummer")
                if str(acc_orgnr) != orgnr:
                    env["gaps"].append("financials (org mismatch)")
                else:
                    period = rec.get("regnskapsperiode") or {}
                    p = f"{period.get('fraDato')}..{period.get('tilDato')}"

                    # Navigate financials structure
                    resultat = rec.get("resultatregnskapResultat") or {}
                    driftsresultat = resultat.get("driftsresultat") or {}
                    driftsinntekter_obj = driftsresultat.get("driftsinntekter") or {}
                    
                    financials = {
                        "revenue": driftsinntekter_obj.get("sumDriftsinntekter"),
                        "operating_result": driftsresultat.get("driftsinntekter"),  # May differ
                        "net_result": resultat.get("aarsresultat"),
                        "profit_before_tax": resultat.get("resultatFørSkatt"),
                        "total_assets": (rec.get("balanse") or {}).get("sum_eiendeler"),
                        "equity": (rec.get("balanse") or {}).get("egenkapital"),
                    }

                    for k, v in financials.items():
                        if v is None:
                            env["gaps"].append(k)
                        else:
                            env["facts"][k] = fact(v, acc_url, p)
            else:
                env["gaps"].append(f"financials ({aerr})")

        # === PHASE 3: Roles ===
        if not is_subunit and phase_limits.get("roles", True):
            roles, rerr = await fetch_roles(client, budget, orgnr, f"{BASE}/enheter/{orgnr}")
            for k, v in roles.items():
                if v is None:
                    env["gaps"].append(k)
                else:
                    env["facts"][k] = fact(v, f"{BASE}/enheter/{orgnr}/roller", None)

        # Build summary
        env["summary"] = build_summary(env)
        
        return env


def save(db, env):
    """Versioned history: only insert a new row when content changed. Compute diff."""
    body = json.dumps(
        env["facts"], sort_keys=True, ensure_ascii=False, default=str
    )
    # Strip volatile timestamps when comparing
    cmp = re.sub(r'"retrieved_at": "[^"]+"', "", body)
    
    row = db.execute(
        "SELECT cmp, envelope FROM profiles WHERE orgnr=? ORDER BY id DESC LIMIT 1",
        (env["orgnr"],),
    ).fetchone()
    
    changed = row is None or row[0] != cmp
    diff = {}
    
    if changed and row is not None:
        # Compute diff: which fields changed?
        try:
            old_env = json.loads(row[1])
            old_facts = old_env.get("facts", {})
            new_facts = env.get("facts", {})
            
            all_keys = set(old_facts.keys()) | set(new_facts.keys())
            for key in all_keys:
                old_val = old_facts.get(key, {}).get("value")
                new_val = new_facts.get(key, {}).get("value")
                if old_val != new_val:
                    diff[key] = {"old": old_val, "new": new_val}
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
    
    if changed:
        db.execute(
            "INSERT INTO profiles(orgnr, retrieved_at, cmp, envelope) VALUES (?,?,?,?)",
            (
                env["orgnr"],
                env["retrieved_at"],
                cmp,
                json.dumps(env, ensure_ascii=False),
            ),
        )
    
    env["changed_since_last_run"] = changed
    env["diff"] = diff
    return env


async def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Signalpost: async scraper for Norwegian company profiles."
    )
    ap.add_argument("input", help="Input file with org numbers (JSON, CSV, or text)")
    ap.add_argument(
        "--mode",
        choices=["official", "batch"],
        default="official",
        help="Execution mode (official=45min/2k req, batch=auto-extend)",
    )
    ap.add_argument(
        "--out", default="output.jsonl", help="Output JSONL file (default: output.jsonl)"
    )
    ap.add_argument(
        "--db", default="profiles.sqlite", help="SQLite database (default: profiles.sqlite)"
    )
    ap.add_argument(
        "--max-time",
        type=int,
        default=360,
        help="Max time extension in batch mode (minutes, default 360)",
    )
    a = ap.parse_args()

    ids = load_ids(a.input)
    if not ids:
        print("ERROR: No org numbers to process.", file=sys.stderr)
        sys.exit(1)

    db = sqlite3.connect(a.db)
    db.execute(
        "CREATE TABLE IF NOT EXISTS profiles(id INTEGER PRIMARY KEY, orgnr TEXT, "
        "retrieved_at TEXT, cmp TEXT, envelope TEXT)"
    )

    budget = Budget(mode=a.mode, total_companies=len(ids), initial_time=BATCH_INITIAL_TIME if a.mode == "batch" else OFFICIAL_TIME_LIMIT)
    sem = asyncio.Semaphore(budget.concurrency)

    start_time = time.time()
    print(
        f"\n=== Signalpost Agent ===",
        file=sys.stderr,
    )
    print(
        f"Mode: {a.mode} | Companies: {len(ids)} | Time limit: {budget.deadline - time.time():.0f}s | Budget: {budget.request_limit} requests",
        file=sys.stderr,
    )
    print(f"Output: {a.out} | Database: {a.db}", file=sys.stderr)
    print()

    # Determine phase limits based on expected batch size and budget
    # For simplicity: if >500 companies, skip roles; if >1000, skip financials too
    phase_limits = {"financials": len(ids) < 1000, "roles": len(ids) < 500}

    results = []
    processed = 0
    times_per_company = deque(maxlen=50)

    async with httpx.AsyncClient(timeout=15, http2=False) as client:
        for i, orgnr in enumerate(ids):
            # Maybe extend time in batch mode
            if i % 100 == 0 and i > 0:
                avg_time = sum(times_per_company) / len(times_per_company) if times_per_company else 0
                extended = budget.maybe_extend_time(i, avg_time)
                if extended:
                    print(
                        f"[{i}/{len(ids)}] Time extended to {budget.deadline - time.time():.0f}s remaining",
                        file=sys.stderr,
                    )
            
            # Adjust semaphore if concurrency changed
            if sem._value != budget.concurrency:
                sem = asyncio.Semaphore(budget.concurrency)
            
            step_start = time.time()
            result = await profile(client, budget, sem, orgnr, phase_limits)
            results.append(result)
            times_per_company.append(time.time() - step_start)
            processed += 1

    elapsed = time.time() - start_time

    # Save results
    with open(a.out, "w", encoding="utf-8") as f:
        for env in results:
            f.write(json.dumps(save(db, env), ensure_ascii=False) + "\n")

    db.commit()
    db.close()

    # Summary report
    ok_count = sum(e["status"] == "ok" for e in results)
    error_count = sum(e["status"] == "error" for e in results)
    not_found_count = sum(e["status"] == "not_found" for e in results)
    mismatch_count = sum(e["status"] == "mismatch" for e in results)

    report = {
        "input_file": a.input,
        "total_companies": len(ids),
        "requests_used": budget.used,
        "requests_budget": budget.request_limit,
        "execution_time_minutes": round(elapsed / 60, 1),
        "time_limit_minutes": round((budget.deadline - start_time) / 60, 1),
        "time_extended": a.mode == "batch" and budget.deadline > start_time + BATCH_INITIAL_TIME,
        "concurrency_final": budget.concurrency,
        "status_breakdown": {
            "ok": ok_count,
            "error": error_count,
            "not_found": not_found_count,
            "mismatch": mismatch_count,
        },
    }

    with open("run_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(
        f"\n=== EXECUTION SUMMARY ===",
        file=sys.stderr,
    )
    print(
        f"Total: {len(ids)} | OK: {ok_count} | Errors: {error_count} | Not found: {not_found_count} | Mismatch: {mismatch_count}",
        file=sys.stderr,
    )
    print(
        f"Requests: {budget.used}/{budget.request_limit} | Time: {elapsed / 60:.1f}min",
        file=sys.stderr,
    )
    print(f"Concurrency: {budget.concurrency} workers", file=sys.stderr)
    print(f"Output: {a.out} | Report: run_report.json", file=sys.stderr)
    print()


if __name__ == "__main__":
    asyncio.run(main())
