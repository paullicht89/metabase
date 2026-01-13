#!/usr/bin/env python3
"""
Dataverse -> Postgres (analytics) sync
- Reads config from /config/.env
- Pulls Dataverse entityset(s) with paging
- Stores raw payload as jsonb into staging tables
- Optional: runs transform SQL (CREATE OR REPLACE TABLE / VIEW etc.) after staging load
"""

from __future__ import annotations

import json
import os
import sys
import time
import argparse
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 is required. Install with: pip install psycopg2-binary", file=sys.stderr)
    raise


ENV_PATH_DEFAULT = "/config/.env"


# -----------------------------
# .env loader (no dependencies)
# -----------------------------
def load_env_file(env_path: str) -> None:
    """
    Loads KEY=VALUE lines into os.environ (does not override existing env vars).
    Supports quoted values. Ignores blank lines and # comments.
    """
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found: {env_path}")

    with open(env_path, "r", encoding="utf-8") as f:
        for raw in f.readlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def require_env(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        raise RuntimeError(f"Missing required env var: {key}")
    return v


def normalize_pg_url(url: str) -> str:
    """
    Accepts:
      - postgresql://...
      - postgres://...
      - postgresql+psycopg2://...
      - postgresql+psycopg://...
    Returns a psycopg2-compatible DSN URL.
    """
    if url.startswith("postgresql+"):
        return "postgresql://" + url.split("://", 1)[1]
    if url.startswith("postgres+"):
        return "postgres://" + url.split("://", 1)[1]
    return url


# -----------------------------
# Dataverse
# -----------------------------
def get_access_token(tenant_id: str, client_id: str, client_secret: str, dv_base_url: str) -> str:
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": f"{dv_base_url}/.default",
    }
    resp = requests.post(token_url, data=data, timeout=60)
    resp.raise_for_status()
    return resp.json()["access_token"]


def dv_get_paged(
    session: requests.Session,
    first_url: str,
    max_pages: Optional[int] = None,
    sleep_s: float = 0.0,
) -> List[Dict[str, Any]]:
    """
    Pulls all pages from a Dataverse OData endpoint.
    Returns the concatenated list from the `value` arrays.
    """
    all_rows: List[Dict[str, Any]] = []
    url = first_url
    pages = 0

    while url:
        pages += 1
        if max_pages is not None and pages > max_pages:
            break

        resp = session.get(url, timeout=120)
        if resp.status_code in (429, 503, 504):
            # basic backoff
            retry_after = int(resp.headers.get("Retry-After", "5"))
            time.sleep(max(retry_after, 5))
            continue

        resp.raise_for_status()
        data = resp.json()
        chunk = data.get("value", [])
        if chunk:
            all_rows.extend(chunk)

        url = data.get("@odata.nextLink")

        if sleep_s:
            time.sleep(sleep_s)

    return all_rows


# -----------------------------
# Postgres staging
# -----------------------------
def ensure_schema(cur, schema: str) -> None:
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')


def ensure_raw_table(cur, schema: str, table: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
            payload  jsonb NOT NULL,
            pulled_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )


def truncate_table(cur, schema: str, table: str) -> None:
    cur.execute(f'TRUNCATE TABLE "{schema}"."{table}";')


def insert_raw_rows(cur, schema: str, table: str, rows: List[Dict[str, Any]]) -> None:
    """
    Bulk insert rows as jsonb.
    """
    if not rows:
        return

    values = [(json.dumps(r),) for r in rows]
    psycopg2.extras.execute_values(
        cur,
        f'INSERT INTO "{schema}"."{table}" (payload) VALUES %s',
        values,
        page_size=2000,
    )


def run_sql(cur, sql: str) -> None:
    """
    Runs arbitrary SQL (you can paste CREATE OR REPLACE TABLE/VIEW here).
    """
    cur.execute(sql)


# -----------------------------
# Main orchestration
# -----------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Sync Dataverse tables into Postgres staging.")
    parser.add_argument("--env", default=ENV_PATH_DEFAULT, help="Path to .env (default: /config/.env)")
    parser.add_argument("--only", nargs="*", default=None, help="Only run these table keys (e.g. new_servloc fsip_maintenancecontract)")
    parser.add_argument("--max-pages", type=int, default=None, help="Max pages to fetch per table (debug/testing)")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between pages (rate limiting)")
    parser.add_argument("--no-transform", action="store_true", help="Do not run transform SQL after staging load")
    args = parser.parse_args()

    load_env_file(args.env)

    dv_client_id = require_env("DV_CLIENT_ID")
    dv_client_secret = require_env("DV_CLIENT_SECRET")
    dv_tenant_id = require_env("DV_TENANT_ID")
    dv_base_url = require_env("DV_BASE_URL").rstrip("/")  # e.g. https://lcdelevator.crm.dynamics.com
    analytics_db_url = normalize_pg_url(require_env("ANALYTICS_DB_URL"))

    # Optional, not used by this script (but you said you have it)
    _metabase_db_url = os.environ.get("METABASE_DB_URL")
    _dv_business_unit = os.environ.get("DV_BUSINESS_UNIT")  # not used unless you later filter by BU

    token = get_access_token(dv_tenant_id, dv_client_id, dv_client_secret, dv_base_url)

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            # REQUIRED header you asked for:
            "Prefer": 'odata.include-annotations="OData.Community.Display.V1.FormattedValue"',
            # Optional but commonly helpful:
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }
    )

    # -----------------------------
    # Configure your tables here
    # -----------------------------
    # key: your friendly name
    # entityset: the Dataverse entity set name (the URL segment)
    # select: optional $select fields (comma separated)
    # filter: optional $filter
    # staging_schema/table: where raw JSON goes
    # transform_sql: paste your CREATE OR REPLACE TABLE... SQL here (optional)
    TABLES: Dict[str, Dict[str, Any]] = {
        "new_servloc": {
            "entityset": "new_servlocs",
            "select": None,  # or "new_servlocid,new_name,..."
            "filter": None,
            "staging_schema": "staging",
            "staging_table": "dv_new_servloc_raw",
            "transform_sql": """CREATE OR REPLACE VIEW dataverse.new_servloc AS
SELECT
    payload->>'new_servlocid' AS new_servlocid,
    payload->>'new_name'      AS new_name,
    payload->>'new_carnumber' AS new_carnumber,
    payload->>'_fsip_buildinglocation_value' AS fsip_buildinglocationid,
    NULLIF(payload->>'statecode','')::int AS statecode,
    payload->>'statecode@OData.Community.Display.V1.FormattedValue' AS statecodename,
    NULLIF(payload->>'statuscode','')::int AS statuscode,
    payload->>'statuscode@OData.Community.Display.V1.FormattedValue' AS statuscodename
FROM staging.dv_new_servloc_raw;""",  # paste SQL later
        },
        "fsip_maintenancecontract": {
            "entityset": "fsip_maintenancecontracts",
            "select": None,
            "filter": None,
            "staging_schema": "staging",
            "staging_table": "dv_fsip_maintenancecontract_raw",
            "transform_sql": """CREATE OR REPLACE VIEW dataverse.fsip_maintenancecontract AS
SELECT
    payload->>'fsip_maintenancecontractid' AS fsip_maintenancecontractid,
    payload->>'fsip_name'      AS fsip_name,
    NULLIF(payload->>'fsip_originalcontractstartdate','')::date AS fsip_originalcontractstartdate,
    NULLIF(payload->>'fsip_currentcontractstartdate','')::date AS fsip_currentcontractstartdate,
    payload->>'fsip_closedstatus@OData.Community.Display.V1.FormattedValue' AS fsip_closedstatusname,
    NULLIF(payload->>'fsip_closedstatus','')::int AS fsip_closedstatus,
    NULLIF(payload->>'fsip_closedate','')::date AS fsip_closedate,
    NULLIF(payload->>'fsip_closecontract','')::boolean AS fsip_closecontract,
    payload->>'new_carnumber' AS fsip_closecontractname,
    payload->>'_fsip_primarybuildinglocation_value' AS fsip_buildinglocationid,
    NULLIF(payload->>'statecode','')::int AS statecode,
    payload->>'statecode@OData.Community.Display.V1.FormattedValue' AS statecodename,
    NULLIF(payload->>'statuscode','')::int AS statuscode,
    payload->>'statuscode@OData.Community.Display.V1.FormattedValue' AS statuscodename
FROM staging.dv_fsip_maintenancecontract_raw;""",
        },
        "fsip_buildinglocations": {
            "entityset": "fsip_buildinglocations",
            "select": None,
            "filter": None,
            "staging_schema": "staging",
            "staging_table": "dv_fsip_buildinglocations_raw",
            "transform_sql": """CREATE OR REPLACE VIEW dataverse.fsip_buildinglocation AS
SELECT
    payload->>'fsip_buildinglocationid' AS fsip_buildinglocationid,
    payload->>'fsip_name'      AS fsip_name,
    payload->>'fsip_street' AS fsip_street,
    NULLIF(payload->>'lcd_region','')::int AS lcd_region,
    payload->>'lcd_region@OData.Community.Display.V1.FormattedValue' AS lcd_regionname,
    payload->>'_fsip_primarytech_value' AS fsip_primarytech,
    payload->>'_fsip_primarytech_value@OData.Community.Display.V1.FormattedValue' AS fsip_primarytechname,
    NULLIF(payload->>'statecode','')::int AS statecode,
    payload->>'statecode@OData.Community.Display.V1.FormattedValue' AS statecodename,
    NULLIF(payload->>'statuscode','')::int AS statuscode,
    payload->>'statuscode@OData.Community.Display.V1.FormattedValue' AS statuscodename
FROM staging.dv_fsip_buildinglocations_raw;""",
        },
        "fsip_maintenancecontract_devices": {
            "entityset": "fsip_maintenancecontract_devicesset",
            "select": None,
            "filter": None,
            "staging_schema": "staging",
            "staging_table": "dv_fsip_maintenancecontract_devices_raw",
            "transform_sql": """CREATE OR REPLACE VIEW dataverse.fsip_maintenancecontract_devices AS
SELECT
    payload->>'new_servlocid' AS new_servlocid,
    payload->>'fsip_maintenancecontractid'      AS fsip_maintenancecontractid,
    payload->>'fsip_maintenancecontract_devicesid' AS fsip_maintenancecontract_devicesid
FROM staging.dv_fsip_maintenancecontract_devices_raw;""",
        },
        "systemusers": {
            "entityset": "systemusers",
            "select": None,
            "filter": None,
            "staging_schema": "staging",
            "staging_table": "dv_systemusers_raw",
            "transform_sql": """CREATE OR REPLACE VIEW dataverse.systemusers AS
SELECT
    payload->>'systemuserid' AS systemuserId,
    payload->>'fullname'      AS fullname,
    payload->>'domainname' AS domainname
FROM staging.dv_systemusers_raw;""",
        },
    }

    # Narrow selection if requested
    run_keys = list(TABLES.keys())
    if args.only:
        missing = [k for k in args.only if k not in TABLES]
        if missing:
            print(f"ERROR: Unknown table keys: {missing}. Valid keys: {run_keys}", file=sys.stderr)
            return 2
        run_keys = args.only

    print(f"Connecting to analytics DB: {analytics_db_url}")
    conn = psycopg2.connect(analytics_db_url)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for key in run_keys:
                cfg = TABLES[key]
                entityset = cfg["entityset"]
                schema = cfg["staging_schema"]
                table = cfg["staging_table"]
                select = cfg.get("select")
                filt = cfg.get("filter")

                # Build initial URL
                base_api = f"{dv_base_url}/api/data/v9.2/"
                url = urljoin(base_api, entityset)

                params = []
                if select:
                    params.append(f"$select={select}")
                if filt:
                    params.append(f"$filter={filt}")
                # (Optional) you can add $top=... but paging will handle full pull
                if params:
                    url = url + "?" + "&".join(params)

                print(f"\n=== Pulling {key} ({entityset}) ===")
                t0 = time.time()
                rows = dv_get_paged(session, url, max_pages=args.max_pages, sleep_s=args.sleep)
                dt = time.time() - t0
                print(f"Fetched {len(rows)} rows in {dt:.1f}s")

                # Ensure staging table exists, then truncate+load
                ensure_schema(cur, schema)
                ensure_raw_table(cur, schema, table)
                truncate_table(cur, schema, table)
                insert_raw_rows(cur, schema, table, rows)

                # Optional transform step
                if not args.no_transform:
                    transform_sql = cfg.get("transform_sql")
                    if transform_sql:
                        print(f"Running transform SQL for {key}...")
                        run_sql(cur, transform_sql)

                conn.commit()
                print(f"Loaded staging: {schema}.{table} ✓")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
