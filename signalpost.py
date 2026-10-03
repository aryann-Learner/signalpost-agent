#!/usr/bin/env python3
"""Signalpost challenge runner.

Deterministic, no-fabrication company profile scraper for Brreg.

Features:
- reads org numbers from JSON/CSV/plain text input
- validates Norwegian org numbers with mod-11 check
- fetches entity + accounts + roles from Brreg endpoints
- keeps exact source URL + retrieval date for every fact
- rejects wrong-company or fabricated values
- stores historical snapshots in SQLite with diffs
- supports official mode and batch mode
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
ACCOUNTS = "https://data.brreg.no/regnskapsregisteret/regnskap"

OFFICIAL_TIME_LIMIT = 42 * 60
OFFICIAL_REQUEST_LIMIT = 1900
DEFAULT_CHUNK_SIZE = 1500
BATCH_EXTEND_LIMIT = 6 * 60 * 60

CONCURRENCY_START = 8
CONCURRENCY_MIN = 2
CONCURRENCY_MAX = 24
THROTTLE_WINDOW = 100
THROTTLE_DOWN = 0.05
THROTTLE_UP = 0.01


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def valid_orgnr(orgnr: str) -> bool:
    if not re.fullmatch(r"\d{9}", orgnr):
        return False
    weights = [3, 2, 7, 6, 5, 4, 3, 2]
    total = sum(int(orgnr[i]) * weights[i] for i in range(8))
    mod = total % 11
    check = 0 if mod == 0 else 11 - mod
    if check == 10:
        return False
    return int(orgnr[8]) == check


def normalize_orgnr_token(token: str) -> str:
    digits = re.sub(r"\D", "", str(token))
    return digits if len(digits) == 9 else ""


def load_ids(path: str) -> List[str]:
    try:
        raw = open(path, "r", encoding="utf-8").read()
    except FileNotFoundError:
        raw = os.environ.get("SIGNALPOST_INPUT", "")
        if not raw:
            print(f"ERROR: input file '{path}' not found and SIGNALPOST_INPUT is empty.", file=sys.stderr)
            sys.exit(1)

    ids: List[str] = []
    seen = set()

    # JSON first
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            for key in ("companies", "orgnrs", "organizations", "data"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    candidate = (
                        item.get("orgnr") or item.get("organisasjonsnummer") or item.get("id") or ""
                    )
                else:
                    candidate = item
                orgnr = normalize_orgnr_token(candidate)
                if orgnr and valid_orgnr(orgnr) and orgnr not in seen:
                    seen.add(orgnr)
                    ids.append(orgnr)
            if ids:
                return ids
    except Exception:
        pass

    # CSV / text fallback
    lines = raw.strip().splitlines()
    if not lines:
        return []

    # If CSV with a header, detect orgnr column
    first_line = lines[0].strip()
    if "," in first_line:
        columns = [c.strip().lower() for c in first_line.split(",")]
        candidate_index = next((i for i, c in enumerate(columns) if "orgnr" in c or "organisasjon" in c), None)
        if candidate_index is not None:
            for line in lines[1:]:
                if not line.strip():
                    continue
                parts = [p.strip() for p in line.split(",")]
                if candidate_index < len(parts):
                    orgnr = normalize_orgnr_token(parts[candidate_index])
                    if orgnr and valid_orgnr(orgnr) and orgnr not in seen:
                        seen.add(orgnr)
                        ids.append(orgnr)
            return ids

    for line in lines:
        for token in line.split():
            orgnr = normalize_orgnr_token(token)
            if orgnr and valid_orgnr(orgnr) and orgnr not in seen:
                seen.add(orgnr)
                ids.append(orgnr)

    return ids


class Budget:
    def __init__(self, mode: str, total_companies: int):
        self.mode = mode
        self.total_companies = total_companies
        self.request_limit = OFFICIAL_REQUEST_LIMIT
        self.used = 0
        self.concurrency = CONCURRENCY_START
        self.deadline = time.time() + OFFICIAL_TIME_LIMIT
        self.initial_deadline = self.deadline
        self.throttle_state = deque(maxlen=THROTTLE_WINDOW)

    def take(self) -> bool:
        if self.used >= self.request_limit:
            return False
        if time.time() >= self.deadline:
            return False
        self.used += 1
        return True

    def record_throttle(self, value: bool):
        self.throttle_state.append(value)
        if len(self.throttle_state) < THROTTLE_WINDOW:
            return
        rate = sum(self.throttle_state) / len(self.throttle_state)
        if rate > THROTTLE_DOWN and self.concurrency > CONCURRENCY_MIN:
            self.concurrency = max(CONCURRENCY_MIN, self.concurrency // 2)
        elif rate < THROTTLE_UP and self.concurrency < CONCURRENCY_MAX:
            self.concurrency = min(CONCURRENCY_MAX, self.concurrency + 2)

    def time_left(self) -> float:
        return max(0.0, self.deadline - time.time())


async def fetch_url(client: httpx.AsyncClient, budget: Budget, url: str, retries: int = 2) -> Tuple[Optional[Any], Optional[str], bool]:
    for attempt in range(retries + 1):
        if not budget.take():
            return None, "budget_exhausted", False
        try:
            r = await client.get(url, headers={"Accept": "application/json"})
            if r.status_code == 200:
                budget.record_throttle(False)
                try:
                    return r.json(), None, False
                except Exception:
                    return None, "invalid_json", False
            if r.status_code == 404:
                budget.record_throttle(False)
                return None, "not_found", False
            if r.status_code == 410:
                budget.record_throttle(False)
                return None, "deleted", False
            if r.status_code in (429, 500, 502, 503):
                budget.record_throttle(True)
                if attempt < retries:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                return None, f"http_{r.status_code}", True
            budget.record_throttle(False)
            return None, f"http_{r.status_code}", False
        except httpx.HTTPError as exc:
            budget.record_throttle(True)
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            return None, f"network:{type(exc).__name__}", True
    return None, "failed", False


def deep_get(obj: Any, *path: str) -> Any:
    current = obj
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(key)]
            except (TypeError, ValueError, IndexError):
                return None
        else:
            return None
        if current is None:
            return None
    return current


def fact(value: Any, source_url: str, reporting_period: Optional[str] = None) -> Dict[str, Any]:
    return {
        "value": value,
        "source_url": source_url,
        "retrieved_at": now_iso(),
        "reporting_period": reporting_period,
    }


def pick_company_accounts(records: List[Dict[str, Any]], orgnr: str) -> Optional[Dict[str, Any]]:
    if not records:
        return None
    matches = [r for r in records if str(deep_get(r, "virksomhet", "organisasjonsnummer") or "") == orgnr]
    if not matches:
        return None
    company_only = [r for r in matches if str(deep_get(r, "regnskapstype") or "").upper() == "SELSKAP"]
    candidates = company_only if company_only else matches
    return max(candidates, key=lambda r: str(deep_get(r, "regnskapsperiode", "tilDato") or ""))


def parse_roles(data: Any) -> Dict[str, Any]:
    roles = {"ceo": None, "board_members": None, "auditor": None}
    if not isinstance(data, dict):
        return roles

    groups = data.get("rollegrupper") or data.get("roles") or []
    if not isinstance(groups, list):
        groups = [groups]

    board = []
    ceo = None
    auditor = None

    for group in groups:
        if not isinstance(group, dict):
            continue
        items = group.get("roller") or group.get("items") or []
        if not isinstance(items, list):
            items = [items]
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("fratraadt") is True:
                continue

            role_type = item.get("type") or {}
            if isinstance(role_type, dict):
                role_name = str(role_type.get("beskrivelse") or role_type.get("kode") or "").lower()
            else:
                role_name = str(role_type).lower()

            person = item.get("person") or {}
            if isinstance(person, dict):
                name_parts = []
                for k in ("fornavn", "mellomnavn", "etternavn", "navn"):
                    val = person.get(k)
                    if val:
                        name_parts.append(str(val))
                person_name = " ".join(name_parts).strip()
            else:
                person_name = str(person).strip()

            if not person_name and isinstance(item.get("navn"), dict):
                nav = item["navn"]
                person_name = " ".join(
                    [str(nav.get(k, "")).strip() for k in ("fornavn", "mellomnavn", "etternavn") if nav.get(k)]
                )
            if not person_name:
                person_name = str(item.get("navn") or "").strip()
            if not person_name:
                continue

            if "styremedlem" in role_name or "styre" in role_name or "board" in role_name:
                board.append(person_name)
            elif "daglig leder" in role_name or "ceo" in role_name or "direkt" in role_name:
                ceo = person_name
            elif "revisor" in role_name or "auditor" in role_name:
                auditor = person_name

    if board:
        roles["board_members"] = board
    if ceo:
        roles["ceo"] = ceo
    if auditor:
        roles["auditor"] = auditor
    return roles


async def profile_company(client: httpx.AsyncClient, budget: Budget, sem: asyncio.Semaphore, orgnr: str) -> Dict[str, Any]:
    async with sem:
        env = {
            "orgnr": orgnr,
            "status": "ok",
            "retrieved_at": now_iso(),
            "facts": {},
            "gaps": [],
            "summary": "",
            "changed_since_last_run": False,
            "diff": {},
        }

        if not valid_orgnr(orgnr):
            env["status"] = "invalid"
            env["summary"] = f"Invalid org number: {orgnr}"
            return env

        entity_url = f"{BASE}/enheter/{orgnr}"
        unit, err, _ = await fetch_url(client, budget, entity_url)
        if unit is None and err == "not_found":
            unit_url = f"{BASE}/underenheter/{orgnr}"
            unit, err, _ = await fetch_url(client, budget, unit_url)
            entity_url = unit_url

        if unit is None:
            env["status"] = "not_found" if err == "not_found" else ("deleted" if err == "deleted" else "error")
            env["error"] = err
            env["summary"] = f"Status: {env['status']}. No profile returned."
            return env

        if str(unit.get("organisasjonsnummer") or "") != orgnr:
            env["status"] = "mismatch"
            env["summary"] = f"Wrong-company guard triggered for {orgnr}."
            return env

        is_subunit = "/underenheter/" in entity_url

        addr = unit.get("forretningsadresse") or unit.get("beliggenhetsadresse") or {}
        org_form = unit.get("organisasjonsform") or {}
        nace1 = unit.get("naeringskode1") or {}
        nace2 = unit.get("naeringskode2") or {}
        nace3 = unit.get("naeringskode3") or {}
        kapital = unit.get("kapital") or {}

        mappings = {
            "navn": unit.get("navn"),
            "organisasjonsnummer": unit.get("organisasjonsnummer"),
            "registreringsdatoEnhetsregisteret": unit.get("registreringsdatoEnhetsregisteret"),
            "stiftelsesdato": unit.get("stiftelsesdato"),
            "antallAnsatte": unit.get("antallAnsatte"),
            "org_form_code": org_form.get("kode"),
            "org_form_description": org_form.get("beskrivelse"),
            "industry_code_1": nace1.get("kode"),
            "industry_description_1": nace1.get("beskrivelse"),
            "industry_code_2": nace2.get("kode"),
            "industry_description_2": nace2.get("beskrivelse"),
            "industry_code_3": nace3.get("kode"),
            "industry_description_3": nace3.get("beskrivelse"),
            "address_street": (addr.get("adresse") or [None])[0] if addr.get("adresse") else None,
            "address_postal": addr.get("postnummer"),
            "address_city": addr.get("poststed"),
            "address_municipality": addr.get("kommune"),
            "address_country": addr.get("landkode"),
            "website": unit.get("hjemmeside"),
            "telefon": unit.get("telefon"),
            "registrertIMvaregisteret": unit.get("registrertIMvaregisteret"),
            "konkurs": unit.get("konkurs"),
            "underAvvikling": unit.get("underAvvikling"),
            "share_capital_amount": kapital.get("belop"),
            "share_capital_currency": kapital.get("valuta"),
            "share_capital_type": kapital.get("type"),
            "number_of_shares": kapital.get("antallAksjer"),
        }

        for key, value in mappings.items():
            if value is None or value == "":
                env["gaps"].append(key)
            else:
                env["facts"][key] = fact(value, entity_url)

        # financials: choose the company-only, latest matching record
        if not is_subunit:
            acc_url = f"{ACCOUNTS}/{orgnr}"
            acc_data, acc_err, _ = await fetch_url(client, budget, acc_url)
            if acc_data:
                records = acc_data if isinstance(acc_data, list) else [acc_data]
                selected = pick_company_accounts(records, orgnr)
                if selected is None:
                    env["gaps"].append("financials (record not found or org mismatch)")
                else:
                    period = selected.get("regnskapsperiode") or {}
                    p = f"{period.get('fraDato')}..{period.get('tilDato')}"
                    financials = {
                        "currency": selected.get("valuta"),
                        "revenue": deep_get(selected, "resultatregnskapResultat", "driftsresultat", "driftsinntekter", "sumDriftsinntekter"),
                        "net_result": deep_get(selected, "resultatregnskapResultat", "aarsresultat"),
                        "profit_before_tax": deep_get(selected, "resultatregnskapResultat", "ordinaertResultatFoerSkattekostnad"),
                        "total_assets": deep_get(selected, "eiendeler", "sumEiendeler"),
                        "equity": deep_get(selected, "egenkapitalGjeld", "egenkapital", "sumEgenkapital"),
                    }
                    for key, value in financials.items():
                        if value is None or value == "":
                            env["gaps"].append(key)
                        elif isinstance(value, (int, float)):
                            env["facts"][key] = fact(value, acc_url, p)
            else:
                env["gaps"].append(f"financials ({acc_err})")

        # roles
        if not is_subunit:
            roles_url = f"{BASE}/enheter/{orgnr}/roller"
            roles_data, roles_err, _ = await fetch_url(client, budget, roles_url)
            if roles_data is not None:
                parsed = parse_roles(roles_data)
                for key, value in parsed.items():
                    if value is None:
                        env["gaps"].append(key)
                    else:
                        env["facts"][key] = fact(value, roles_url)
            elif roles_err:
                env["gaps"].append(f"roles ({roles_err})")

        # summary
        name = env["facts"].get("navn", {}).get("value") or "Unknown company"
        org_form = env["facts"].get("org_form_description", {}).get("value") or ""
        registered = env["facts"].get("registreringsdatoEnhetsregisteret", {}).get("value")
        employees = env["facts"].get("antallAnsatte", {}).get("value")
        revenue = env["facts"].get("revenue", {}).get("value")
        net_result = env["facts"].get("net_result", {}).get("value")
        ceo = env["facts"].get("ceo", {}).get("value")
        board = env["facts"].get("board_members", {}).get("value") or []
        gaps = env.get("gaps", [])

        summary_parts = []
        summary_parts.append(f"{name} ({org_form})" if org_form else name)
        if registered:
            summary_parts.append(f"registered {registered[:10]}")
        if employees is not None:
            summary_parts.append(f"{employees} employees")
        if revenue is not None:
            summary_parts.append(f"revenue {revenue}")
        if ceo:
            summary_parts.append(f"CEO {ceo}")
        if board:
            summary_parts.append(f"board {', '.join(board[:2])}")
        if gaps:
            summary_parts.append(f"missing: {', '.join(gaps[:4])}")
        env["summary"] = ". ".join(summary_parts)
        return env


def save_to_db(db: sqlite3.Connection, env: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(env["facts"], sort_keys=True, ensure_ascii=False, default=str)
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
            for key in sorted(set(old_facts) | set(env["facts"])):
                old_val = old_facts.get(key, {}).get("value")
                new_val = env["facts"].get(key, {}).get("value")
                if old_val != new_val:
                    diff[key] = {"old": old_val, "new": new_val}
        except Exception:
            pass

    if changed:
        db.execute(
            "INSERT INTO profiles(orgnr, retrieved_at, cmp, envelope) VALUES (?, ?, ?, ?)",
            (env["orgnr"], env["retrieved_at"], cmp, json.dumps(env, ensure_ascii=False, default=str)),
        )

    env["changed_since_last_run"] = changed
    env["diff"] = diff
    return env


async def run_batch(client: httpx.AsyncClient, budget: Budget, ids: List[str]) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(budget.concurrency)
    return list(await asyncio.gather(*(profile_company(client, budget, sem, orgnr) for orgnr in ids)))


async def main():
    ap = argparse.ArgumentParser(description="Signalpost challenge runner")
    ap.add_argument("input", nargs="?", help="Input file with org numbers or use SIGNALPOST_INPUT")
    ap.add_argument("--mode", choices=["official", "batch"], default="official")
    ap.add_argument("--out", default="output.jsonl")
    ap.add_argument("--db", default="profiles.sqlite")
    ap.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    ap.add_argument("--max-time", type=int, default=360)
    args = ap.parse_args()

    input_file = args.input or os.environ.get("SIGNALPOST_INPUT")
    if not input_file:
        print("ERROR: no input file specified", file=sys.stderr)
        sys.exit(1)

    ids = load_ids(input_file)
    if not ids:
        print("ERROR: no valid org numbers were found", file=sys.stderr)
        sys.exit(1)

    db = sqlite3.connect(args.db)
    db.execute(
        "CREATE TABLE IF NOT EXISTS profiles(id INTEGER PRIMARY KEY, orgnr TEXT, retrieved_at TEXT, cmp TEXT, envelope TEXT)"
    )

    start = time.time()
    results: List[Dict[str, Any]] = []
    budget = Budget(mode=args.mode, total_companies=len(ids))

    if args.mode == "official":
        async with httpx.AsyncClient(timeout=15, http2=False) as client:
            results = await run_batch(client, budget, ids)
    else:
        for i in range(0, len(ids), args.chunk_size):
            chunk = ids[i:i + args.chunk_size]
            async with httpx.AsyncClient(timeout=15, http2=False) as client:
                chunk_results = await run_batch(client, budget, chunk)
            results.extend(chunk_results)
            print(f"processed chunk {i // args.chunk_size + 1} ({len(chunk)} ids)", file=sys.stderr)

    with open(args.out, "w", encoding="utf-8") as f:
        for env in results:
            env = save_to_db(db, env)
            f.write(json.dumps(env, ensure_ascii=False, default=str) + "\n")
    db.commit()
    db.close()

    elapsed = time.time() - start
    print(f"Processed {len(ids)} org numbers in {elapsed:.1f}s", file=sys.stderr)
    print(f"Requests used: {budget.used}/{budget.request_limit}", file=sys.stderr)
    print(f"Output -> {args.out}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
