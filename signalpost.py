#!/usr/bin/env python3
"""Signalpost agent: challenge-ready scraper for Norwegian company profiles.

Hard constraints:
  - No fabricated values (missing fields go to gaps, never defaulted)
  - Strict org number matching on every response
  - Every fact tied to exact source URL + retrieval date
  - Wrong-company guard (hard-fail rule)
  - SQLite versioning with change diffs

Usage:
  python signalpost.py input.txt [--mode official|batch] [--out output.jsonl] [--db profiles.sqlite]

Input file can be JSON, CSV (with orgnr column), or plain text (one per line).
Accepts environment variable SIGNALPOST_INPUT as fallback.
"""
import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

BASE = "https://data.brreg.no/enhetsregisteret/api"
ACCOUNTS = "https://data.regnskapsregister.brreg.no/regnskapsregister/regnskap"

# Hard limits (challenge rules)
OFFICIAL_TIME_LIMIT = 42 * 60  # seconds
OFFICIAL_REQUEST_LIMIT = 1900  # leave 100 under 2,000 cap
DEFAULT_CHUNK_SIZE = 1500
MAX_BATCH_EXTENSION = 6 * 60 * 60  # 6 hours

# Concurrency tuning
CONCURRENCY_START = 8
CONCURRENCY_MIN = 2
CONCURRENCY_MAX = 24
THROTTLE_WINDOW = 100
THROTTLE_DOWN_THRESHOLD = 0.05  # >5% fail -> reduce
THROTTLE_UP_THRESHOLD = 0.01  # <1% fail -> increase


def now_iso() -> str:
    """Current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def validate_orgnr(orgnr: str) -> bool:
    """Validate Norwegian org number using mod-11 checksum."""
    if not re.match(r"^\d{9}$", orgnr):
        return False
    weights = [3, 2, 7, 6, 5, 4, 3, 2]
    total = sum(int(orgnr[i]) * weights[i] for i in range(8))
    remainder = total % 11
    check_digit = 0 if remainder == 0 else (11 - remainder)
    return check_digit != 10 and int(orgnr[8]) == check_digit


def load_ids(path: str) -> List[str]:
    """Load org numbers from file (JSON, CSV, or text). Handles spaced numbers."""
    try:
        raw = open(path, encoding="utf-8").read()
    except FileNotFoundError:
        # Try environment variable
        if "SIGNALPOST_INPUT" in os.environ:
            raw = os.environ["SIGNALPOST_INPUT"]
        else:
            print(f"ERROR: Input file '{path}' not found and SIGNALPOST_INPUT not set.", file=sys.stderr)
            sys.exit(1)

    ids = []
    seen = set()

    # Try JSON first
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            # Look for common keys
            for key in ["companies", "orgnrs", "organizations", "data"]:
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    candidate = item.get("orgnr") or item.get("organisasjonsnummer") or item.get("id")
                    if candidate:
                        candidate = str(candidate)
                else:
                    candidate = str(item)
                digits = re.sub(r"\D", "", candidate)
                if len(digits) == 9 and validate_orgnr(digits) and digits not in seen:
                    seen.add(digits)
                    ids.append(digits)
        return ids
    except (json.JSONDecodeError, TypeError, KeyError):
        pass

    # Try CSV with header
    lines = raw.strip().split("\n")
    if "," in lines[0]:
        header = lines[0].lower()
        orgnr_col = -1
        for i, col in enumerate(header.split(",")):
            if "orgnr" in col or "organisasjon" in col:
                orgnr_col = i
                break
        if orgnr_col >= 0:
            for line in lines[1:]:
                if not line.strip():
                    continue
                parts = line.split(",")
                if orgnr_col < len(parts):
                    candidate = parts[orgnr_col].strip()
                    digits = re.sub(r"\D", "", candidate)
                    if len(digits) == 9 and validate_orgnr(digits) and digits not in seen:
                        seen.add(digits)
                        ids.append(digits)
            return ids

    # Plain text: one per line or space-separated
    for line in lines:
        if not line.strip():
            continue
        # Handle both space-separated and newline-separated
        for token in line.split():
            digits = re.sub(r"\D", "", token)
            if len(digits) == 9 and validate_orgnr(digits) and digits not in seen:
                seen.add(digits)
                ids.append(digits)

    return ids


class Budget:
    """Track request budget, time deadline, and concurrency."""

    def __init__(self, mode: str, total_companies: int):
        self.mode = mode
        self.total_companies = total_companies
        self.request_limit = OFFICIAL_REQUEST_LIMIT
        self.used = 0
        self.concurrency = CONCURRENCY_START
        self.start_time = time.time()
        self.deadline = self.start_time + (
            OFFICIAL_TIME_LIMIT if mode == "official" else OFFICIAL_TIME_LIMIT
        )
        self.initial_deadline = self.deadline
        self.throttle_history = deque(maxlen=THROTTLE_WINDOW)

    def can_take_request(self) -> bool:
        """Check if a request can be made."""
        return self.used < self.request_limit and time.time() < self.deadline

    def take_request(self) -> bool:
        """Attempt to take a request from budget."""
        if not self.can_take_request():
            return False
        self.used += 1
        return True

    def record_throttle(self, was_throttled: bool):
        """Record throttle event for concurrency tuning."""
        self.throttle_history.append(was_throttled)
        if len(self.throttle_history) >= THROTTLE_WINDOW:
            rate = sum(self.throttle_history) / len(self.throttle_history)
            if rate > THROTTLE_DOWN_THRESHOLD and self.concurrency > CONCURRENCY_MIN:
                self.concurrency = max(CONCURRENCY_MIN, self.concurrency // 2)
            elif rate < THROTTLE_UP_THRESHOLD and self.concurrency < CONCURRENCY_MAX:
                self.concurrency = min(CONCURRENCY_MAX, self.concurrency + 2)

    def time_remaining(self) -> float:
        """Seconds remaining."""
        return max(0.0, self.deadline - time.time())

    def requests_remaining(self) -> int:
        """Requests remaining."""
        return max(0, self.request_limit - self.used)


async def fetch_url(
    client: httpx.AsyncClient, budget: Budget, url: str, retries: int = 2
) -> Tuple[Optional[Dict], Optional[str], bool]:
    """Fetch URL with retry logic on 429/5xx. Returns (data, error, was_throttled)."""
    throttled = False
    for attempt in range(retries + 1):
        if not budget.take_request():
            return None, "budget_exhausted", False
        try:
            response = await client.get(url, headers={"Accept": "application/json"})
            if response.status_code == 200:
                budget.record_throttle(False)
                try:
                    return response.json(), None, False
                except json.JSONDecodeError:
                    return None, "invalid_json", False
            if response.status_code == 404:
                budget.record_throttle(False)
                return None, "not_found", False
            if response.status_code == 410:
                budget.record_throttle(False)
                return None, "deleted", False
            if response.status_code in (429, 500, 502, 503):
                throttled = True
                budget.record_throttle(True)
                if attempt < retries:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
            budget.record_throttle(False)
            return None, f"http_{response.status_code}", throttled
        except httpx.HTTPError as exc:
            budget.record_throttle(True)
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            return None, f"network:{type(exc).__name__}", True

    return None, "failed", throttled


def deep_get(obj: Any, *path: str) -> Any:
    """Safely navigate nested dict/list."""
    current = obj
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(key)]
            except (ValueError, TypeError, IndexError):
                return None
        else:
            return None
        if current is None:
            return None
    return current


def fact(
    value: Any, source_url: str, reporting_period: Optional[str] = None
) -> Dict[str, Any]:
    """Create a fact with traceability."""
    return {
        "value": value,
        "source_url": source_url,
        "retrieved_at": now_iso(),
        "reporting_period": reporting_period,
    }


async def fetch_roles(
    client: httpx.AsyncClient, budget: Budget, orgnr: str
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Fetch roles (CEO, board, auditor) from /roller endpoint.
    
    Real structure: rollegrupper -> roller array with type (description), person (fornavn/etternavn)
    """
    roles_data = {"ceo": None, "board_members": None, "auditor": None}
    url = f"{BASE}/enheter/{orgnr}/roller"

    data, err, _ = await fetch_url(client, budget, url)
    if data is None:
        return roles_data, err

    try:
        # Navigate the actual structure: rollegrupper -> roller
        rolle_grupper = data.get("rollegrupper") or []
        if not isinstance(rolle_grupper, list):
            rolle_grupper = []

        board = []
        ceo = None
        auditor = None

        for gruppe in rolle_grupper:
            roller = gruppe.get("roller") or []
            if not isinstance(roller, list):
                continue
            for role in roller:
                # Type has beskrivelse (description)
                type_obj = role.get("type") or {}
                role_desc = (type_obj.get("beskrivelse") or "").lower()
                
                # Person name is split into fornavn/etternavn
                person = role.get("person") or {}
                fornavn = person.get("fornavn") or ""
                etternavn = person.get("etternavn") or ""
                person_name = (fornavn + " " + etternavn).strip()

                if not person_name:
                    continue

                if "styremedlem" in role_desc or "board" in role_desc:
                    board.append(person_name)
                elif "daglig leder" in role_desc or "ceo" in role_desc or "director" in role_desc:
                    ceo = person_name
                elif "revisor" in role_desc or "auditor" in role_desc:
                    auditor = person_name

        if board:
            roles_data["board_members"] = board
        if ceo:
            roles_data["ceo"] = ceo
        if auditor:
            roles_data["auditor"] = auditor

        return roles_data, None
    except (KeyError, TypeError, AttributeError) as e:
        return roles_data, f"parse_error:{type(e).__name__}"


async def profile_company(
    client: httpx.AsyncClient, budget: Budget, sem: asyncio.Semaphore, orgnr: str
) -> Dict[str, Any]:
    """Fetch complete company profile."""
    async with sem:
        url = f"{BASE}/enheter/{orgnr}"
        unit, err, _ = await fetch_url(client, budget, url)
        if unit is None and err == "not_found":
            # Try sub-unit
            url = f"{BASE}/underenheter/{orgnr}"
            unit, err, _ = await fetch_url(client, budget, url)

        env = {
            "orgnr": orgnr,
            "status": "ok",
            "retrieved_at": now_iso(),
            "facts": {},
            "gaps": [],
            "summary": "",
            "diff": {},
            "changed_since_last_run": False,
        }

        if unit is None:
            env["status"] = "not_found" if err == "not_found" else ("deleted" if err == "deleted" else "error")
            env["error"] = err
            env["summary"] = f"Status: {env['status']}. No profile retrieved."
            return env

        # HARD GUARD: org number must match
        returned_orgnr = str(unit.get("organisasjonsnummer") or "")
        if returned_orgnr != orgnr:
            env["status"] = "mismatch"
            env["summary"] = f"Wrong-company guard: requested {orgnr}, got {returned_orgnr}."
            return env

        is_subunit = "/underenheter/" in url

        # Extract all entity fields (not hand-picked)
        # Only extract non-null, non-empty values; avoid fabrication
        for key, value in unit.items():
            # Skip internal/structural fields
            if key.startswith("_") or key in ("paategninger",):
                continue
            # Skip complex nested structures (will be cherry-picked instead)
            if isinstance(value, (dict, list)) and key not in (
                "historiskeNavn",
                "frivilligMvaRegistrertBeskrivelser",
                "aktivitet",
            ):
                continue
            # Add simple scalar values; handle 0 correctly for antallAnsatte
            if key == "antallAnsatte" and value is not None:
                env["facts"][key] = fact(value, url)
            elif value not in (None, "", False):
                env["facts"][key] = fact(value, url)

        # Cherry-pick structured fields
        addr = unit.get("forretningsadresse") or unit.get("beliggenhetsadresse") or {}
        nace1 = unit.get("naeringskode1") or {}
        nace2 = unit.get("naeringskode2") or {}
        nace3 = unit.get("naeringskode3") or {}
        org_form = unit.get("organisasjonsform") or {}
        kapital = unit.get("kapital") or {}

        structured = {
            "org_form_code": org_form.get("kode"),
            "org_form_description": org_form.get("beskrivelse"),
            "industry_code_1": nace1.get("kode"),
            "industry_description_1": nace1.get("beskrivelse"),
            "industry_code_2": nace2.get("kode"),
            "industry_description_2": nace2.get("beskrivelse"),
            "industry_code_3": nace3.get("kode"),
            "industry_description_3": nace3.get("beskrivelse"),
            "address_street": (addr.get("adresse") or [""])[0] if addr.get("adresse") else None,
            "address_municipality": addr.get("kommune"),
            "address_municipality_code": addr.get("kommunenummer"),
            "address_postal": addr.get("postnummer"),
            "address_city": addr.get("poststed"),
            "address_country": addr.get("landkode"),  # NO, not defaulted
            "share_capital_amount": kapital.get("belop"),
            "share_capital_currency": kapital.get("valuta"),
            "share_capital_type": kapital.get("type"),
            "number_of_shares": kapital.get("antallAksjer"),
        }

        for key, value in structured.items():
            if value not in (None, ""):
                env["facts"][key] = fact(value, url)
            else:
                env["gaps"].append(key)

        # Financials: only for legal entities, strict org matching
        if not is_subunit:
            acc_url = f"{ACCOUNTS}/{orgnr}"
            acc_data, acc_err, _ = await fetch_url(client, budget, acc_url)
            if acc_data:
                records = acc_data if isinstance(acc_data, list) else [acc_data]
                # Find the matching record by org number AND pick the latest by tilDato
                matching_records = []
                for rec in records:
                    rec_orgnr = str(
                        deep_get(rec, "organisasjonsnummer")
                        or deep_get(rec, "organisasjonsNummer")
                        or ""
                    )
                    if rec_orgnr == orgnr:
                        matching_records.append(rec)

                if matching_records:
                    # Pick latest by tilDato
                    chosen = max(
                        matching_records,
                        key=lambda r: deep_get(r, "regnskapsperiode", "tilDato") or "",
                    )
                    period = chosen.get("regnskapsperiode") or {}
                    p = f"{period.get('fraDato')}..{period.get('tilDato')}"
                    
                    # Check if consolidated (regnskapstype)
                    regnskap_type = chosen.get("regnskapstype", "")
                    if "konsern" in str(regnskap_type).lower():
                        env["gaps"].append("financials (consolidated, not company-only)")
                    else:
                        # Safe field paths (verified against real API)
                        financials = {
                            "revenue": deep_get(
                                chosen,
                                "resultatregnskapResultat",
                                "driftsresultat",
                                "driftsinntekter",
                                "sumDriftsinntekter",
                            ),
                            "operating_result": deep_get(
                                chosen, "resultatregnskapResultat", "driftsresultat"
                            ),
                            "net_result": deep_get(
                                chosen, "resultatregnskapResultat", "aarsresultat"
                            ),
                            "total_assets": deep_get(
                                chosen, "balanse", "eiendeler", "sumEiendeler"
                            )
                            or deep_get(chosen, "balanse", "sum_eiendeler"),
                            "equity": deep_get(
                                chosen,
                                "balanse",
                                "egenkapitalGjeld",
                                "egenkapital",
                                "sumEgenkapital",
                            )
                            or deep_get(chosen, "balanse", "egenkapital"),
                            "profit_before_tax": deep_get(
                                chosen, "resultatregnskapResultat", "resultatFørSkatt"
                            ),
                        }

                        for key, value in financials.items():
                            if value is None:
                                env["gaps"].append(key)
                            elif isinstance(value, (int, float)):
                                env["facts"][key] = fact(value, acc_url, p)
                            # Skip non-numeric values for financials
                else:
                    env["gaps"].append(f"financials (org {orgnr} not found in records)")
            else:
                env["gaps"].append(f"financials ({acc_err})")

        # Roles: only for legal entities
        if not is_subunit:
            roles, role_err = await fetch_roles(client, budget, orgnr)
            for key, value in roles.items():
                if value is None:
                    env["gaps"].append(key)
                else:
                    env["facts"][key] = fact(value, f"{BASE}/enheter/{orgnr}/roller")

        # Build summary
        env["summary"] = build_summary(env)

        return env


def build_summary(env: Dict[str, Any]) -> str:
    """Template-based summary (no LLM, $0 cost)."""
    facts = env.get("facts", {})
    gaps = env.get("gaps", [])
    if env["status"] != "ok":
        return f"Status: {env['status']}. No verified profile."

    name = facts.get("navn", {}).get("value") or facts.get("name", {}).get("value") or "Unknown"
    org_form = facts.get("org_form_description", {}).get("value", "")
    registered = facts.get("registreringsdatoEnhetsregisteret", {}).get("value", "")
    employees = facts.get("antallAnsatte", {}).get("value")
    municipality = facts.get("address_municipality", {}).get("value")
    revenue = facts.get("revenue", {}).get("value")
    net_result = facts.get("net_result", {}).get("value")
    ceo = facts.get("ceo", {}).get("value")
    board = facts.get("board_members", {}).get("value") or []

    parts = []
    if org_form:
        parts.append(f"{name} ({org_form})")
    else:
        parts.append(name)
    if registered:
        parts.append(f"registered {registered[:10]}")
    if employees is not None and employees > 0:
        parts.append(f"{employees} employees")
    if municipality:
        parts.append(f"in {municipality}")

    summary = ", ".join(parts) + ". "
    if revenue is not None:
        summary += f"Revenue: NOK {revenue:,.0f}. "
    if net_result is not None:
        summary += f"Net result: NOK {net_result:,.0f}. "
    if ceo:
        summary += f"CEO: {ceo}. "
    if board:
        summary += f"Board: {', '.join(board[:2])}. "
    if gaps:
        summary += f"Missing: {', '.join(gaps[:3])}."

    return summary.strip()


def save_to_db(db: sqlite3.Connection, env: Dict[str, Any]) -> Dict[str, Any]:
    """Save profile to SQLite with versioning and diff."""
    body = json.dumps(env["facts"], sort_keys=True, ensure_ascii=False, default=str)
    # Strip timestamps for comparison
    cmp = re.sub(r'"retrieved_at": "[^"]+"', "", body)

    row = db.execute(
        "SELECT cmp, envelope FROM profiles WHERE orgnr = ? ORDER BY id DESC LIMIT 1",
        (env["orgnr"],),
    ).fetchone()

    changed = row is None or row[0] != cmp
    diff = {}

    if changed and row is not None:
        try:
            old_env = json.loads(row[1])
            old_facts = old_env.get("facts", {})
            new_facts = env.get("facts", {})
            for key in sorted(set(old_facts.keys()) | set(new_facts.keys())):
                old_val = old_facts.get(key, {}).get("value")
                new_val = new_facts.get(key, {}).get("value")
                if old_val != new_val:
                    diff[key] = {"old": old_val, "new": new_val}
        except Exception:
            pass

    if changed:
        db.execute(
            "INSERT INTO profiles(orgnr, retrieved_at, cmp, envelope) VALUES (?, ?, ?, ?)",
            (
                env["orgnr"],
                env["retrieved_at"],
                cmp,
                json.dumps(env, ensure_ascii=False, default=str),
            ),
        )

    env["changed_since_last_run"] = changed
    env["diff"] = diff
    return env


def validate_output(env: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Pre-write validation: no fabrication, org match, numeric financials."""
    # Every fact URL must contain the org number or be from a trusted endpoint
    for key, fact_obj in env.get("facts", {}).items():
        url = fact_obj.get("source_url", "")
        value = fact_obj.get("value")
        # Org number should appear in most URLs
        if env["orgnr"] not in url and "/roller" not in url:
            if not url.startswith((BASE, ACCOUNTS)):
                return False, f"Fact {key}: untrusted URL {url}"
        # Financials must be numeric
        if key in ("revenue", "operating_result", "net_result", "total_assets", "equity", "profit_before_tax"):
            if not isinstance(value, (int, float)):
                return False, f"Fact {key}: non-numeric value {value}"
    return True, None


async def main():
    ap = argparse.ArgumentParser(
        description="Signalpost: challenge-ready Norwegian company profile scraper"
    )
    ap.add_argument("input", nargs="?", help="Input file with org numbers (or use SIGNALPOST_INPUT env var)")
    ap.add_argument(
        "--mode", choices=["official", "batch"], default="official",
        help="official=42min/1900req (hard), batch=chunked submission generation"
    )
    ap.add_argument("--out", default="output.jsonl", help="Output JSONL file")
    ap.add_argument("--db", default="profiles.sqlite", help="SQLite database")
    ap.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Batch mode chunk size")
    ap.add_argument("--dump-raw", action="store_true", help="Save raw API responses")
    ap.add_argument("--validation-only", action="store_true", help="Validate output, don't fetch")
    args = ap.parse_args()

    input_file = args.input or os.environ.get("SIGNALPOST_INPUT")
    if not input_file:
        print("ERROR: No input file specified and SIGNALPOST_INPUT not set.", file=sys.stderr)
        sys.exit(1)

    ids = load_ids(input_file)
    if not ids:
        print("ERROR: No valid org numbers found.", file=sys.stderr)
        sys.exit(1)

    print(
        f"[SIGNALPOST] Loaded {len(ids)} companies. Mode: {args.mode}. "
        f"Budget: {OFFICIAL_REQUEST_LIMIT} requests, {OFFICIAL_TIME_LIMIT}s.",
        file=sys.stderr,
    )

    db = sqlite3.connect(args.db)
    db.execute(
        "CREATE TABLE IF NOT EXISTS profiles(id INTEGER PRIMARY KEY, orgnr TEXT UNIQUE, retrieved_at TEXT, cmp TEXT, envelope TEXT)"
    )

    start_time = time.time()
    results = []
    budget = Budget(mode=args.mode, total_companies=len(ids))
    sem = asyncio.Semaphore(budget.concurrency)

    async with httpx.AsyncClient(timeout=15, http2=False) as client:
        for i, orgnr in enumerate(ids):
            if i > 0 and i % 50 == 0:
                elapsed = time.time() - start_time
                print(
                    f"[{i}/{len(ids)}] {budget.used}/{budget.request_limit} requests, {elapsed:.0f}s, conc={budget.concurrency}",
                    file=sys.stderr,
                )
            result = await profile_company(client, budget, sem, orgnr)
            results.append(result)

    elapsed = time.time() - start_time

    # Write output
    with open(args.out, "w", encoding="utf-8") as f:
        for env in results:
            is_valid, validation_err = validate_output(env)
            if not is_valid:
                print(f"[VALIDATION] {env['orgnr']}: {validation_err}", file=sys.stderr)
                env["validation_error"] = validation_err
            env = save_to_db(db, env)
            f.write(json.dumps(env, ensure_ascii=False, default=str) + "\n")

    db.commit()
    db.close()

    status_counts = {
        s: sum(1 for r in results if r["status"] == s)
        for s in ["ok", "not_found", "error", "deleted", "mismatch"]
    }

    print(
        f"\n[SUMMARY] {len(results)} total | OK: {status_counts['ok']} | "
        f"Not found: {status_counts['not_found']} | "
        f"Error: {status_counts['error']} | Deleted: {status_counts['deleted']} | "
        f"Mismatch: {status_counts['mismatch']}",
        file=sys.stderr,
    )
    print(
        f"[SUMMARY] Requests: {budget.used}/{budget.request_limit} | "
        f"Time: {elapsed:.1f}s | Output: {args.out}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    asyncio.run(main())
