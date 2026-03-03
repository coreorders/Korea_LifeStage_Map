#!/usr/bin/env python3
"""Daily sync for MOIS legal-dong population OpenAPI (only when new month exists)."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import unquote, urlencode
from urllib.request import Request, urlopen

API_URL = "https://apis.data.go.kr/1741000/stdgSexdAgePpltn/selectStdgSexdAgePpltn"
DEFAULT_SIDO_STDG_CODES = [
    "1100000000",  # 서울
    "2600000000",  # 부산
    "2700000000",  # 대구
    "2800000000",  # 인천
    "2900000000",  # 광주
    "3000000000",  # 대전
    "3100000000",  # 울산
    "3600000000",  # 세종
    "4100000000",  # 경기
    "4200000000",  # 강원
    "4300000000",  # 충북
    "4400000000",  # 충남
    "4500000000",  # 전북
    "4600000000",  # 전남
    "4700000000",  # 경북
    "4800000000",  # 경남
    "5000000000",  # 제주
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync legal-dong population data to sqlite.")
    parser.add_argument("--db-path", default="data/population.db")
    parser.add_argument("--month", default="", help="YYYYMM (optional)")
    parser.add_argument("--auto-month", action="store_true", help="Find newest month with available data")
    parser.add_argument("--lookback-months", type=int, default=6, help="Used with --auto-month")
    parser.add_argument("--only-new", action="store_true", help="Skip when target month already synced")
    parser.add_argument("--stdg-cd", default="0000000000")
    parser.add_argument(
        "--stdg-cd-list",
        default="",
        help="Comma-separated stdgCd list for full collection sweep",
    )
    parser.add_argument("--full-collection", action="store_true", help="Sweep all default sido codes")
    parser.add_argument("--lv", default="3", help="1~7, default 3 (읍면동 단위)")
    parser.add_argument("--reg-se-cd", default="1", help="1 전체 / 2 거주자 / 3 거주불명자 / 4 재외국민")
    parser.add_argument("--num-of-rows", type=int, default=1000)
    parser.add_argument("--max-pages", type=int, default=0, help="0 means all pages")
    parser.add_argument("--save-raw", action="store_true")
    parser.add_argument("--allow-empty", action="store_true", help="Do not fail when zero rows fetched")
    return parser.parse_args()


def api_keys_or_exit() -> list[str]:
    raw = os.getenv("PUBLIC_DATA_API_KEY") or os.getenv("DATA_GO_KR_SERVICE_KEY")
    if not raw:
        print(
            "Missing API key. Set PUBLIC_DATA_API_KEY (or DATA_GO_KR_SERVICE_KEY).",
            file=sys.stderr,
        )
        sys.exit(2)

    raw = raw.strip().strip('"').strip("'")
    decoded = unquote(raw)
    keys: list[str] = []
    for candidate in (raw, decoded):
        if candidate and candidate not in keys:
            keys.append(candidate)
    if keys:
        return keys
    print(
        "Missing API key. Set PUBLIC_DATA_API_KEY (or DATA_GO_KR_SERVICE_KEY).",
        file=sys.stderr,
    )
    sys.exit(2)


def ensure_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
          run_id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_month TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          total_pages INTEGER NOT NULL DEFAULT 0,
          total_items INTEGER NOT NULL DEFAULT 0,
          error_message TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS population_items (
          run_month TEXT NOT NULL,
          stats_ym TEXT,
          stdg_cd TEXT,
          admm_cd TEXT,
          row_key TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          fetched_at TEXT NOT NULL,
          PRIMARY KEY (run_month, row_key)
        )
        """
    )
    return conn


def get_latest_synced_month(path: Path) -> str:
    if not path.exists():
        return ""
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(MAX(run_month), '')
            FROM sync_runs
            WHERE status = 'success' AND total_items > 0
            """
        )
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else ""
    except sqlite3.OperationalError:
        return ""
    finally:
        conn.close()


def get_target_stdg_codes(args: argparse.Namespace) -> list[str]:
    if args.stdg_cd_list.strip():
        codes = [c.strip() for c in args.stdg_cd_list.split(",") if c.strip()]
        return list(dict.fromkeys(codes))
    if args.full_collection:
        return DEFAULT_SIDO_STDG_CODES[:]
    return [args.stdg_cd]


def parse_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    """
    Supports both common formats:
    1) response.head + response.items.item
    2) response.header + response.body.items.item
    """
    root = payload.get("response", payload)

    # header/head
    head = root.get("head", root.get("header", {}))
    if isinstance(head, list) and head:
        head = head[0]
    if not isinstance(head, dict):
        head = {}

    # body/items
    body = root.get("body", root)
    items = body.get("items", root.get("items", {})) if isinstance(body, dict) else {}
    if isinstance(items, list) and items:
        items = items[0]

    item = []
    if isinstance(items, dict):
        item = items.get("item", [])
    elif isinstance(items, list):
        item = items
    if isinstance(item, dict):
        item = [item]
    if not isinstance(item, list):
        item = []

    # totalCount can appear in head/body/root
    total_count = to_int(
        body.get("totalCount", head.get("totalCount", root.get("totalCount", 0)))
        if isinstance(body, dict)
        else head.get("totalCount", root.get("totalCount", 0)),
        default=0,
    )

    return head, item, total_count


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(str(value).replace(",", "").strip())
    except Exception:
        return default


def pick(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def make_row_key(item: dict[str, Any]) -> str:
    parts = [
        pick(item, ["statsYm", "statsYM"]),
        pick(item, ["stdgCd", "stdgcd"]),
        pick(item, ["admmCd", "admmcd"]),
        pick(item, ["dongNm", "dongnm"]),
        pick(item, ["tong"]),
        pick(item, ["ban"]),
    ]
    joined = "|".join(parts).strip("|")
    if joined:
        return joined
    payload = json.dumps(item, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def fetch_page(params: dict[str, Any]) -> dict[str, Any]:
    query = urlencode(params, doseq=True)
    request = Request(f"{API_URL}?{query}", headers={"User-Agent": "kids-birth-sync/1.0"})
    with urlopen(request, timeout=40) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_page_with_keys(params: dict[str, Any], service_keys: list[str]) -> dict[str, Any]:
    last_error: Exception | None = None
    for key in service_keys:
        p = dict(params)
        p["serviceKey"] = key
        try:
            return fetch_page(p)
        except HTTPError as exc:
            if exc.code == 401:
                last_error = exc
                continue
            raise
    if last_error:
        raise last_error
    raise RuntimeError("No valid service key candidate")


def save_raw(payload: dict[str, Any], month: str, page_no: int) -> None:
    raw_dir = Path("data/raw") / month
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"page_{page_no:04d}.json"
    raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def yyyymm_from_date(d: dt.date) -> str:
    return d.strftime("%Y%m")


def add_months(first_day: dt.date, delta_months: int) -> dt.date:
    year = first_day.year + (first_day.month - 1 + delta_months) // 12
    month = (first_day.month - 1 + delta_months) % 12 + 1
    return dt.date(year, month, 1)


def resolve_month_candidates(args: argparse.Namespace) -> list[str]:
    if args.month:
        return [args.month]
    base = dt.date.today().replace(day=1)
    lookback = max(args.lookback_months, 1)
    return [yyyymm_from_date(add_months(base, -1 - i)) for i in range(lookback)]


def fetch_all_items(
    *,
    month: str,
    stdg_code: str,
    lv: str,
    reg_se_cd: str,
    num_of_rows: int,
    max_pages: int,
    service_keys: list[str],
    save_raw_flag: bool,
) -> tuple[list[dict[str, Any]], int]:
    all_items: list[dict[str, Any]] = []
    page_no = 1
    page_count = 0
    while True:
        params = {
            "stdgCd": stdg_code,
            "srchFrYm": month,
            "srchToYm": month,
            "lv": lv,
            "regSeCd": reg_se_cd,
            "numOfRows": str(num_of_rows),
            "pageNo": str(page_no),
            "type": "json",
        }
        payload = fetch_page_with_keys(params, service_keys)
        head, items, parsed_total_count = parse_payload(payload)
        result_code = str(head.get("resultCode", "")).strip()
        result_msg = str(head.get("resultMsg", "")).strip()
        if result_code and result_code != "00":
            raise RuntimeError(f"API error code={result_code}, msg={result_msg}")

        if save_raw_flag:
            save_raw(payload, month, page_no)

        if page_no == 1:
            print(
                f"[probe] month={month} lv={lv} stdgCd={stdg_code} "
                f"totalCount={parsed_total_count} page1_items={len(items)}"
            )

        all_items.extend(items)
        page_count += 1
        page_items = len(items)
        if page_items == 0:
            break
        if parsed_total_count > 0 and len(all_items) >= parsed_total_count:
            break
        if max_pages > 0 and page_no >= max_pages:
            break

        page_no += 1
        time.sleep(0.2)

    return all_items, page_count


def discover_codes_for_lv3(
    *, month: str, sido_codes: list[str], args: argparse.Namespace, service_keys: list[str]
) -> list[str]:
    discovered: set[str] = set()
    for sido_code in sido_codes:
        items, _ = fetch_all_items(
            month=month,
            stdg_code=sido_code,
            lv="2",
            reg_se_cd=args.reg_se_cd,
            num_of_rows=args.num_of_rows,
            max_pages=args.max_pages,
            service_keys=service_keys,
            save_raw_flag=False,
        )
        for item in items:
            code = pick(item, ["stdgCd", "stdgcd"])
            if code:
                discovered.add(code)
    return sorted(discovered)


def main() -> int:
    args = parse_args()
    service_keys = api_keys_or_exit()
    db_path = Path(args.db_path)
    stdg_codes = get_target_stdg_codes(args)

    months_to_try = resolve_month_candidates(args)
    target_month = months_to_try[0]

    if args.only_new:
        latest_synced = get_latest_synced_month(db_path)
        if latest_synced and latest_synced >= target_month:
            print(
                f"Skip: latest synced month={latest_synced}, target month={target_month} (no new data)"
            )
            return 0

    conn = ensure_db(db_path)
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sync_runs(run_month, started_at, status) VALUES (?, ?, ?)",
        (target_month, now, "running"),
    )
    run_id = cur.lastrowid
    conn.commit()

    total_items = 0
    total_pages = 0
    try:
        selected_month = ""
        for month_candidate in months_to_try:
            month_items = 0
            month_pages = 0
            print(f"[month] trying {month_candidate}")
            working_codes = stdg_codes
            working_lv = args.lv
            if args.full_collection and args.lv == "3":
                sigungu_codes = discover_codes_for_lv3(
                    month=month_candidate, sido_codes=stdg_codes, args=args, service_keys=service_keys
                )
                if sigungu_codes:
                    working_codes = sigungu_codes
                    print(
                        f"[discovery] month={month_candidate} lv=2 discovered sigungu codes: {len(working_codes)}"
                    )
                else:
                    print(
                        f"[discovery] month={month_candidate} no lv=2 codes found; fallback to sido codes"
                    )

            for stdg_code in working_codes:
                print(f"[target] month={month_candidate} stdgCd={stdg_code} lv={args.lv}")
                items, pages = fetch_all_items(
                    month=month_candidate,
                    stdg_code=stdg_code,
                    lv=working_lv,
                    reg_se_cd=args.reg_se_cd,
                    num_of_rows=args.num_of_rows,
                    max_pages=args.max_pages,
                    service_keys=service_keys,
                    save_raw_flag=args.save_raw,
                )
                fetched_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                for item in items:
                    row_key = make_row_key(item)
                    payload_json = json.dumps(item, ensure_ascii=False, sort_keys=True)
                    stats_ym = pick(item, ["statsYm", "statsYM"])
                    stdg_cd = pick(item, ["stdgCd", "stdgcd"])
                    admm_cd = pick(item, ["admmCd", "admmcd"])
                    cur.execute(
                        """
                        INSERT INTO population_items
                        (run_month, stats_ym, stdg_cd, admm_cd, row_key, payload_json, fetched_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(run_month, row_key) DO UPDATE SET
                          stats_ym = excluded.stats_ym,
                          stdg_cd = excluded.stdg_cd,
                          admm_cd = excluded.admm_cd,
                          payload_json = excluded.payload_json,
                          fetched_at = excluded.fetched_at
                        WHERE population_items.payload_json <> excluded.payload_json
                        """,
                        (month_candidate, stats_ym, stdg_cd, admm_cd, row_key, payload_json, fetched_at),
                    )
                conn.commit()
                target_items = len(items)
                month_items += target_items
                month_pages += pages
                print(
                    f"[stdgCd {stdg_code}] pages={pages} items={target_items} month_items={month_items}"
                )

            if month_items > 0:
                selected_month = month_candidate
                total_items = month_items
                total_pages = month_pages
                target_month = month_candidate
                break

            print(f"[month] no data for {month_candidate}, trying older month...")

        if total_items == 0 and not args.allow_empty:
            raise RuntimeError(
                f"No rows fetched in candidate months: {', '.join(months_to_try)}. "
                "Check service key/parameters (stdgCd, lv, regSeCd)."
            )

        done_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        cur.execute(
            """
            UPDATE sync_runs
               SET finished_at = ?, status = ?, total_pages = ?, total_items = ?
             WHERE run_id = ?
            """,
            (done_at, "success", total_pages, total_items, run_id),
        )
        conn.commit()
        print(f"Sync complete: month={target_month}, pages={total_pages}, items={total_items}")
        return 0
    except Exception as exc:
        done_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        cur.execute(
            """
            UPDATE sync_runs
               SET finished_at = ?, status = ?, total_pages = ?, total_items = ?, error_message = ?
             WHERE run_id = ?
            """,
            (done_at, "failed", total_pages, total_items, str(exc), run_id),
        )
        conn.commit()
        print(f"Sync failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
