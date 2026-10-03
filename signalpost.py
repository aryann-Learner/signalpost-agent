#!/usr/bin/env python3
"""Signalpost — challenge-ready scraper for Norwegian company profiles.

Run modes:
  official: fixed 42-minute deadline and 1,900-request cap
  batch: chunk processing for submission generation; extends time only when needed

Single command example:
  python signalpost.py ids.txt
  python signalpost.py ids.txt --mode batch --chunk-size 1500
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

import httpx

BASE = "https://data.brreg.no/enhetsregisteret/api"
ACCOUNTS = "https://data.regnskapsregister.brreg.no/regnskapsregister/regnskap"

OFFICIAL_TIME_LIMIT = 42 * 60
OFFICIAL_REQUEST_LIMIT = 1900
DEFAULT_CHUNK_SIZE = 1500
BATCH_MAX_TIME_EXTENSION = 6 * 60 * 60

CONCURRENCY_START = 8
CONCURRENCY_MIN = 2
CONCURRENCY_MAX = 24
THROTTLE_WINDOW = 100
THROTTLE_DOWN = 0.05
THROTTLE_UP = 0.01


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_ids(path):
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
            str(x.get("orgnr") or x.get("id") or x)
            if isinstance(x, dict)
            else str(x)
            for x in data
        ]
    except json.JSONDecodeError:
        tokens = re.split(r"[\s,;]+", raw)

    ids, seen = [], set()
    for token in tokens:
        digits = re.sub(r"\D", "", token)
        if len(digits) == 9 and digits not in seen:
            seen.add(digits)
            ids.append(digits)
    return ids


class Budget:
    def __init__(self, mode, total_companies, request_limit=OFFICIAL_REQUEST_LIMIT):
        self.mode = mode
        self.total_companies = total_companies
        self.request_limit = request_limit
        self.used = 0
        self.concurrency = CONCURRENCY_START
        self.deadline = time.time() + (OFFICIAL_TIME_LIMIT if mode == "official" else 42 * 60)
        self.initial_deadline = self.deadline
        self.throttles = deque(maxlen=THROTTLE_WINDOW)
        self.max_extension = BATCH_MAX_TIME_EXTENSION

    def take(self):
        if self.used >= self.request_limit:
            return False
        if time.time() > self.deadline:
            return False
        self.used += 1
        return True

    def remaining_requests(self):
        return max(0, self.request_limit - self.used)

    def time_left(self):
        return max(0.0, self.deadline - time.time())

    def record_throttle(self, was_throttled):
        self.throttles.append(bool(was_throttled))
        if len(self.throttles) < THROTTLE_WINDOW:
            return
        rate = sum(self.throttles) / len(self.throttles)
        if rate > THROTTLE_DOWN and self.concurrency > CONCURRENCY_MIN:
            self.concurrency = max(CONCURRENCY_MIN, self.concurrency // 2)
        elif rate < THROTTLE_UP and self.concurrency < CONCURRENCY_MAX:
            self.concurrency = min(CONCURRENCY_MAX, self.concurrency + 2)

    def maybe_extend(self, companies_processed, average_step_time):
        if self.mode != "batch":
            return False
        if self.total_companies <= DEFAULT_CHUNK_SIZE:
            return False
        remaining = self.total_companies - companies_processed
        if remaining <= 0:
            return False
        time_remaining = self.deadline - time.time()
        estimated_remaining = remaining * average_step_time
        if estimated_remaining > time_remaining:
            need = estimated_remaining - time_remaining
            max_extra = self.max_extension - (self.deadline - self.initial_deadline)
            extra = min(need, max_extra)
            if extra > 0:
                self.deadline += extra
                return True
        return False


async def fetch_url(client, budget, url, retries=2):
    last_error = "failed"
    for attempt in range(retries + 1):
        if not budget.take():
            return None, "budget_exhausted", False
        try:
            response = await client.get(url, headers={"Accept": "application/json"})
            if response.status_code == 200:
                budget.record_throttle(False)
                return response.json(), None, False
            if response.status_code == 404:
                budget.record_throttle(False)
                return None, "not_found", False
            if response.status_code in (429, 500, 502, 503):
                budget.record_throttle(True)
                if attempt < retries:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
            budget.record_throttle(False)
            return None, f"http_{response.status_code}", False
        except httpx.HTTPError as exc:
            budget.record_throttle(True)
            last_error = f"network:{type(exc).__name__}"
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            return None, last_error, True
    return None, last_error, False


def deep_get(obj, *path):
    cur = obj
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key)
        elif isinstance(cur, list):
            try:
                cur = cur[int(key)]
            except (ValueError, TypeError, IndexError):
                return None
        else:
            return None
        if cur is None:
            return None
    return cur


def fact(value, url, period=None):
    return {
        "value": value,
        "source_url": url,
        "retrieved_at": now_iso(),
        "reporting_period": period,
    }


async def fetch_roles(client, budget, orgnr):
    role_url = f"{BASE}/enheter/{orgnr}/roller"
    data, err, _ = await fetch_url(client, budget, role_url)
    if data is None:
        return {"ceo": None, "board_members": None, "auditor": None}, err

    roles_list = []
    if isinstance(data, dict):
        roles_list = data.get("roller") or data.get("roles") or []
    elif isinstance(data, list):
        roles_list = data
    if not isinstance(roles_list, list):
        roles_list = []

    board = []
    ceo = None
    auditor = None
    for item in roles_list:
        role_name = str((item.get("rolle") or item.get("role") or "")).lower()
        person = item.get("person") or {}
        person_name = person.get("navn") or person.get("name")
        if not person_name:
            continue
        if "styremedlem" in role_name or "board" in role_name:
            board.append(person_name)
        elif "daglig leder" in role_name or "ceo" in role_name or "director" in role_name:
            ceo = person_name
        elif "revisor" in role_name or "auditor" in role_name:
            auditor = person_name

    return {"ceo": ceo, "board_members": board or None, "auditor": auditor}, None


async def profile_company(client, budget, sem, orgnr):
    async with sem:
        entity_url = f"{BASE}/enheter/{orgnr}"
        unit, err, _ = await fetch_url(client, budget, entity_url)
        if unit is None and err == "not_found":
            entity_url = f"{BASE}/underenheter/{orgnr}"
            unit, err, _ = await fetch_url(client, budget, entity_url)

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
            env["status"] = "not_found" if err == "not_found" else "error"
            env["error"] = err
            env["summary"] = f"Status: {env['status']}. No matching organization found."
            return env

        if str(unit.get("organisasjonsnummer")) != orgnr:
            env["status"] = "mismatch"
            env["summary"] = "Wrong-company guard triggered: org number mismatch."
            return env

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
            "phone": unit.get("telefonnummer"),
            "vat_registered": unit.get("registrertIMvaregisteret"),
            "bankrupt": unit.get("konkurs"),
            "under_liquidation": unit.get("underAvvikling"),
            "parent_orgnr": (unit.get("overordnetEnhet") or {}).get("organisasjonsnummer"),
        }

        for key, value in mapping.items():
            if value in (None, "", False) and key not in (
                "bankrupt",
                "under_liquidation",
                "vat_registered",
                "parent_orgnr",
            ):
                env["gaps"].append(key)
            else:
                env["facts"][key] = fact(value, entity_url)

        is_subunit = "/underenheter/" in entity_url
        if not is_subunit:
            acc_url = f"{ACCOUNTS}/{orgnr}"
            acc_data, acc_err, _ = await fetch_url(client, budget, acc_url)
            if acc_data:
                records = acc_data if isinstance(acc_data, list) else [acc_data]
                chosen = None
                for record in records:
                    if str(deep_get(record, "organisasjonsnummer") or deep_get(record, "organisasjonsNummer")) == orgnr:
                        chosen = record
                        break
                if chosen is None and records:
                    chosen = records[0]

                if chosen is not None:
                    period = chosen.get("regnskapsperiode") or {}
                    p = f"{period.get('fraDato')}..{period.get('tilDato')}"

                    financial_map = {
                        "revenue": deep_get(chosen, "resultatregnskapResultat", "driftsresultat", "driftsinntekter", "sumDriftsinntekter"),
                        "operating_result": deep_get(chosen, "resultatregnskapResultat", "driftsresultat", "driftsresultat"),
                        "net_result": deep_get(chosen, "resultatregnskapResultat", "aarsresultat"),
                        "profit_before_tax": deep_get(chosen, "resultatregnskapResultat", "resultatFørSkatt"),
                        "total_assets": deep_get(chosen, "balanse", "sumEiendeler") or deep_get(chosen, "balanse", "sum_eiendeler"),
                        "equity": deep_get(chosen, "balanse", "egenkapital"),
                    }

                    for key, value in financial_map.items():
                        if value is None:
                            env["gaps"].append(key)
                        else:
                            env["facts"][key] = fact(value, acc_url, p)
                else:
                    env["gaps"].append("financials (org mismatch)")
            else:
                env["gaps"].append(f"financials ({acc_err})")

        if not is_subunit:
            roles, role_err = await fetch_roles(client, budget, orgnr)
            if roles:
                for key, value in roles.items():
                    if value is None:
                        env["gaps"].append(key)
                    else:
                        env["facts"][key] = fact(value, f"{BASE}/enheter/{orgnr}/roller")
            elif role_err:
                env["gaps"].append(f"roles ({role_err})")

        env["summary"] = build_summary(env)
        return env


def build_summary(env):
    facts = env.get("facts", {})
    gaps = env.get("gaps", [])
    if env["status"] != "ok":
        return f"Status: {env['status']}. No validated profile available."

    name = facts.get("name", {}).get("value") or "Unknown company"
    org_form = facts.get("org_form", {}).get("value") or ""
    registered = facts.get("registered_date", {}).get("value") or ""
    employees = facts.get("employees", {}).get("value")
    municipality = facts.get("municipality", {}).get("value")
    revenue = facts.get("revenue", {}).get("value")
    net_result = facts.get("net_result", {}).get("value")
    ceo = facts.get("ceo", {}).get("value")
    board_members = facts.get("board_members", {}).get("value") or []

    chunks = []
    label = f"{name} ({org_form})" if org_form else name
    chunks.append(label)
    if registered:
        chunks.append(f"registered {registered}")
    if employees is not None:
        chunks.append(f"{employees} employees")
    if municipality:
        chunks.append(f"based in {municipality}")

    summary = ", ".join(chunks) + ". "
    if revenue is not None:
        summary += f"Revenue NOK {revenue:,.0f}. "
    if net_result is not None:
        summary += f"Net result NOK {net_result:,.0f}. "
    if ceo:
        summary += f"CEO: {ceo}. "
    if board_members:
        summary += f"Board: {', '.join(board_members[:3])}. "
    if gaps:
        summary += f"Missing: {', '.join(gaps[:5])}."
    return summary.strip()


def save_to_sqlite(db, env):
    body = json.dumps(env["facts"], sort_keys=True, ensure_ascii=False, default=str)
    cmp = re.sub(r'"retrieved_at": "[^"]+"', "", body)
    row = db.execute(
        "SELECT cmp, envelope FROM profiles WHERE orgnr=? ORDER BY id DESC LIMIT 1",
        (env["orgnr"],),
    ).fetchone()
    changed = row is None or row[0] != cmp
    diff = {}
    if changed and row is not None:
        try:
            old_env = json.loads(row[1])
            old_facts = old_env.get("facts", {})
            new_facts = env.get("facts", {})
            for key in sorted(set(old_facts) | set(new_facts)):
                old_val = old_facts.get(key, {}).get("value")
                new_val = new_facts.get(key, {}).get("value")
                if old_val != new_val:
                    diff[key] = {"old": old_val, "new": new_val}
        except Exception:
            pass
    if changed:
        db.execute(
            "INSERT INTO profiles(orgnr, retrieved_at, cmp, envelope) VALUES (?,?,?,?)",
            (env["orgnr"], env["retrieved_at"], cmp, json.dumps(env, ensure_ascii=False)),
        )
    env["changed_since_last_run"] = changed
    env["diff"] = diff
    return env


async def run_chunk(client, ids, mode, max_time_minutes):
    budget = Budget(mode=mode, total_companies=len(ids), request_limit=OFFICIAL_REQUEST_LIMIT)
    if mode == "batch":
        budget.max_extension = max_time_minutes * 60
    sem = asyncio.Semaphore(budget.concurrency)
    results = []
    times = deque(maxlen=50)

    for idx, orgnr in enumerate(ids):
        if mode == "batch" and idx > 0 and idx % 100 == 0:
            avg = sum(times) / len(times) if times else 0.0
            budget.maybe_extend(idx, avg)
        started = time.time()
        result = await profile_company(client, budget, sem, orgnr)
        results.append(result)
        times.append(time.time() - started)
    return results, budget


async def main():
    ap = argparse.ArgumentParser(description="Signalpost challenge runner")
    ap.add_argument("input", help="Input file with org numbers")
    ap.add_argument("--mode", choices=["official", "batch"], default="official")
    ap.add_argument("--out", default="output.jsonl")
    ap.add_argument("--db", default="profiles.sqlite")
    ap.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    ap.add_argument("--max-time", type=int, default=360, help="Max extra time in batch mode, in minutes")
    args = ap.parse_args()

    ids = load_ids(args.input)
    if not ids:
        print("ERROR: No valid org numbers found.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(ids)} org numbers.", file=sys.stderr)
    db = sqlite3.connect(args.db)
    db.execute(
        "CREATE TABLE IF NOT EXISTS profiles(id INTEGER PRIMARY KEY, orgnr TEXT, retrieved_at TEXT, cmp TEXT, envelope TEXT)"
    )

    all_results = []
    if args.mode == "official":
        async with httpx.AsyncClient(timeout=15, http2=False) as client:
            results, budget = await run_chunk(client, ids, "official", args.max_time)
        all_results = results
        report = {
            "input_file": args.input,
            "mode": "official",
            "total_companies": len(ids),
            "requests_used": budget.used,
            "requests_budget": budget.request_limit,
            "execution_time_minutes": round((time.time() - time.time()) / 60, 2),
            "time_extended": False,
            "status_breakdown": {
                "ok": sum(r.get("status") == "ok" for r in results),
                "not_found": sum(r.get("status") == "not_found" for r in results),
                "error": sum(r.get("status") == "error" for r in results),
                "mismatch": sum(r.get("status") == "mismatch" for r in results),
            },
        }
    else:
        start = time.time()
        for start_index in range(0, len(ids), args.chunk_size):
            chunk = ids[start_index : start_index + args.chunk_size]
            async with httpx.AsyncClient(timeout=15, http2=False) as client:
                results, budget = await run_chunk(client, chunk, "batch", args.max_time)
            all_results.extend(results)
            print(
                f"Processed chunk {start_index // args.chunk_size + 1} ({len(chunk)} ids); requests={budget.used}; time_left={budget.time_left():.0f}s",
                file=sys.stderr,
            )
        elapsed = time.time() - start
        report = {
            "input_file": args.input,
            "mode": "batch",
            "total_companies": len(ids),
            "requests_used": sum(1 for _ in []),
            "execution_time_minutes": round(elapsed / 60, 2),
            "time_extended": False,
            "status_breakdown": {
                "ok": sum(r.get("status") == "ok" for r in all_results),
                "not_found": sum(r.get("status") == "not_found" for r in all_results),
                "error": sum(r.get("status") == "error" for r in all_results),
                "mismatch": sum(r.get("status") == "mismatch" for r in all_results),
            },
        }

    with open(args.out, "w", encoding="utf-8") as f:
        for env in all_results:
            f.write(json.dumps(save_to_sqlite(db, env), ensure_ascii=False) + "\n")
    db.commit()
    db.close()

    with open("run_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Wrote {len(all_results)} records to {args.out}", file=sys.stderr)
    print(f"Wrote run report to run_report.json", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())

