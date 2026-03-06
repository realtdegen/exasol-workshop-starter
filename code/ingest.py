"""
Ingest NHS Prescribing Data into Exasol - Staged version

Usage:
    uv run python ingest.py stage -t 2 -n 3
    uv run python ingest.py finalize
    uv run python ingest.py cleanup
    uv run python ingest.py summary
    uv run python ingest.py query
"""

import argparse
import json
import pyexasol
import ssl
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path

DEPLOYMENT_DIR = Path.home() / "deployment"
URLS_FILE = "prescription_urls.json"
SCHEMA = "PRESCRIPTIONS_UK"


def get_config():
    dep_files = list(DEPLOYMENT_DIR.glob("deployment-exasol-*.json"))
    sec_files = list(DEPLOYMENT_DIR.glob("secrets-exasol-*.json"))
    if not dep_files or not sec_files:
        raise FileNotFoundError("No deployment/secrets files found in ~/deployment/")
    with open(dep_files[0]) as f:
        deploy = json.load(f)
    with open(sec_files[0]) as f:
        secrets = json.load(f)
    node = deploy["nodes"]["n11"]
    return {
        "host": node["dnsName"],
        "port": int(node["database"]["dbPort"]),
        "user": secrets["dbUsername"],
        "password": secrets["dbPassword"],
    }


def connect():
    cfg = get_config()
    conn = pyexasol.connect(
        dsn="{}:{}".format(cfg["host"], cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        encryption=True,
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
    )
    conn.execute("CREATE SCHEMA IF NOT EXISTS {}".format(SCHEMA))
    conn.execute("OPEN SCHEMA {}".format(SCHEMA))
    return conn


print_lock = Lock()


def safe_print(msg):
    with print_lock:
        print(msg)


def load_one_staging_table(cfg, url_info, max_retries=3):
    csv_url = url_info["url"]
    period = url_info["period"]
    size_mb = url_info["size_mb"]
    table_name = "STG_EPD_{}".format(period)

    safe_print("[{}] Starting ({:.0f} MB)...".format(period, size_mb))

    for attempt in range(max_retries):
        try:
            start = time.time()
            conn = pyexasol.connect(
                dsn="{}:{}".format(cfg["host"], cfg["port"]),
                user=cfg["user"],
                password=cfg["password"],
                encryption=True,
                websocket_sslopt={"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
            )
            conn.execute("OPEN SCHEMA {}".format(SCHEMA))

            conn.execute("DROP TABLE IF EXISTS {}".format(table_name))
            conn.execute("""
            CREATE TABLE {} (
                YEAR_MONTH DECIMAL(18,0),
                REGIONAL_OFFICE_NAME VARCHAR(200),
                REGIONAL_OFFICE_CODE VARCHAR(20),
                STP_NAME VARCHAR(200),
                STP_CODE VARCHAR(20),
                PCO_NAME VARCHAR(200),
                PCO_CODE VARCHAR(20),
                PRACTICE_NAME VARCHAR(200),
                PRACTICE_CODE VARCHAR(20),
                ADDRESS_1 VARCHAR(200),
                ADDRESS_2 VARCHAR(200),
                ADDRESS_3 VARCHAR(200),
                ADDRESS_4 VARCHAR(200),
                POSTCODE VARCHAR(20),
                BNF_CHEMICAL_SUBSTANCE VARCHAR(20),
                CHEMICAL_SUBSTANCE_BNF_DESCR VARCHAR(200),
                BNF_CODE VARCHAR(20),
                BNF_DESCRIPTION VARCHAR(200),
                BNF_CHAPTER_PLUS_CODE VARCHAR(100),
                QUANTITY DECIMAL(18,2),
                ITEMS DECIMAL(18,0),
                TOTAL_QUANTITY DECIMAL(18,2),
                ADQUSAGE DECIMAL(18,2),
                NIC DECIMAL(18,2),
                ACTUAL_COST DECIMAL(18,2),
                UNIDENTIFIED VARCHAR(10)
            )
            """.format(table_name))

            base_url = re.sub(r"/download/[^/]+$", "/download/", csv_url)
            filename = csv_url.split("/")[-1]

            conn_name = "NHS_BSA_CONN_{}".format(period)
            conn.execute("DROP CONNECTION IF EXISTS {}".format(conn_name))
            conn.execute("CREATE CONNECTION {} TO '{}'".format(conn_name, base_url))

            conn.execute("""
            IMPORT INTO {}
            FROM CSV AT {}
            FILE '{}'
            COLUMN SEPARATOR = ','
            ROW SEPARATOR = 'CRLF'
            SKIP = 1
            ENCODING = 'UTF8'
            """.format(table_name, conn_name, filename))

            rows = conn.execute("SELECT COUNT(*) FROM {}".format(table_name)).fetchone()[0]
            elapsed = time.time() - start
            safe_print("[{}] Done: {:,} rows in {:.1f}s ({:,.0f} rows/sec)".format(
                period, rows, elapsed, rows / elapsed))
            conn.close()
            return (table_name, rows, None)

        except Exception as e:
            error_msg = str(e)
            is_transient = any(s in error_msg for s in ["42636", "Connection reset", "Recv failure"])
            if attempt < max_retries - 1 and is_transient:
                wait = (attempt + 1) * 5
                safe_print("[{}] Retry {}/{} in {}s...".format(period, attempt + 1, max_retries, wait))
                time.sleep(wait)
            else:
                safe_print("[{}] ERROR: {}".format(period, e))
                return (table_name, 0, str(e))

    return (table_name, 0, "max retries exceeded")


def cmd_stage(args):
    print("LOADING STAGING TABLES")

    with open(URLS_FILE) as f:
        urls = json.load(f)

    cfg = get_config()
    conn = connect()

    existing = {row[0] for row in conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' AND table_name LIKE 'STG_EPD_%'".format(SCHEMA)
    ).fetchall()}
    conn.close()

    if existing:
        print("Found {} existing staging tables".format(len(existing)))

    limit = min(args.num_months, len(urls)) if args.num_months else len(urls)
    to_load = [u for u in urls[:limit]
               if "STG_EPD_{}".format(u["period"]) not in existing or args.force]

    if not to_load:
        print("All requested months already loaded. Use --force to reload.")
        return

    print("Loading {} months with {} threads".format(len(to_load), args.threads))

    results = []
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {executor.submit(load_one_staging_table, cfg, u): u for u in to_load}
        for future in as_completed(futures):
            results.append(future.result())

    success = [r for r in results if r[2] is None]
    failed = [r for r in results if r[2] is not None]
    total_rows = sum(r[1] for r in success)
    print("Done: {} success, {} failed, {:,} total rows".format(
        len(success), len(failed), total_rows))
    if failed:
        for name, _, err in failed:
            print("  FAILED: {} - {}".format(name, err))


def cmd_finalize(args):
    print("CREATING MAIN TABLES FROM STAGING")

    conn = connect()
    staging = [row[0] for row in conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' AND table_name LIKE 'STG_EPD_%' "
        "ORDER BY table_name".format(SCHEMA)
    ).fetchall()]

    if not staging:
        print("No staging tables found. Run 'stage' first.")
        conn.close()
        return

    print("Found {} staging tables".format(len(staging)))

    conn.execute("DROP TABLE IF EXISTS PRESCRIPTIONS")
    conn.execute("""
    CREATE TABLE PRESCRIPTIONS (
        PRACTICE VARCHAR(20),
        BNF_CODE VARCHAR(20),
        BNF_NAME VARCHAR(200),
        ITEMS DECIMAL(18,0),
        NIC DECIMAL(18,2),
        ACT_COST DECIMAL(18,2),
        QUANTITY DECIMAL(18,0),
        PERIOD DECIMAL(18,0),
        POSTCODE VARCHAR(20)
    )
    """)

    total = 0
    for table in staging:
        start = time.time()
        conn.execute("""
            INSERT INTO PRESCRIPTIONS
            (PRACTICE, BNF_CODE, BNF_NAME, ITEMS, NIC, ACT_COST, QUANTITY, PERIOD, POSTCODE)
            SELECT PRACTICE_CODE, BNF_CODE, TRIM(BNF_DESCRIPTION), ITEMS, NIC, ACTUAL_COST,
                   QUANTITY, YEAR_MONTH, POSTCODE
            FROM {}
        """.format(table))
        rows = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        elapsed = time.time() - start
        total += rows
        print("  {}: {:,} rows in {:.1f}s".format(table, rows, elapsed))

    print("Total: {:,} rows".format(total))
    conn.close()


def cmd_cleanup(args):
    print("DROPPING STAGING TABLES")
    conn = connect()
    for row in conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' AND table_name LIKE 'STG_EPD_%'".format(SCHEMA)
    ).fetchall():
        conn.execute("DROP TABLE IF EXISTS {}".format(row[0]))
        print("  Dropped: {}".format(row[0]))
    for row in conn.execute(
        "SELECT connection_name FROM EXA_ALL_CONNECTIONS "
        "WHERE connection_name LIKE 'NHS_BSA_CONN%'"
    ).fetchall():
        conn.execute("DROP CONNECTION IF EXISTS {}".format(row[0]))
    conn.close()
    print("Done!")


def cmd_summary(args):
    conn = connect()
    tables = conn.execute(
        "SELECT table_name FROM EXA_ALL_TABLES "
        "WHERE table_schema = '{}' ORDER BY table_name".format(SCHEMA)
    ).fetchall()
    if not tables:
        print("No tables found.")
    else:
        for row in tables:
            count = conn.execute("SELECT COUNT(*) FROM {}".format(row[0])).fetchone()[0]
            print("  {}: {:,}".format(row[0], count))
    conn.close()


def cmd_query(args):
    conn = connect()
    print("Top 3 most prescribed chemicals in East Central London (EC postcodes):")
    result = conn.execute("""
        SELECT BNF_NAME, SUM(ITEMS) AS total
        FROM PRESCRIPTIONS
        WHERE POSTCODE LIKE 'EC%'
        GROUP BY BNF_NAME
        ORDER BY total DESC
        LIMIT 3
    """).fetchall()
    for i, row in enumerate(result, 1):
        print("  {}. {}: {:,}".format(i, row[0], row[1]))

    if result:
        top = result[0][0]
        print("\nYear with most prescriptions of '{}':".format(top))
        yr = conn.execute("""
            SELECT FLOOR(PERIOD / 100) AS yr, SUM(ITEMS) AS total
            FROM PRESCRIPTIONS
            WHERE POSTCODE LIKE 'EC%%' AND BNF_NAME = '{}'
            GROUP BY FLOOR(PERIOD / 100)
            ORDER BY total DESC
            LIMIT 1
        """.format(top)).fetchone()
        if yr:
            print("  Year: {}, Prescriptions: {:,}".format(int(yr[0]), yr[1]))
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="NHS Prescribing Data Ingestion")
    parser.add_argument("command", choices=["stage", "finalize", "cleanup", "summary", "query"])
    parser.add_argument("-t", "--threads", type=int, default=2)
    parser.add_argument("-n", "--num-months", type=int, default=None)
    parser.add_argument("-f", "--force", action="store_true")
    args = parser.parse_args()

    cmds = {
        "stage": cmd_stage,
        "finalize": cmd_finalize,
        "cleanup": cmd_cleanup,
        "summary": cmd_summary,
        "query": cmd_query,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
