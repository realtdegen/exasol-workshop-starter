"""
Export warehouse tables to Parquet files.

Usage:
    uv run python export_parquet.py [--output-dir data/parquet]
"""

import argparse
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyexasol

import utils.db as db


TABLES = ["PRACTICE", "CHEMICAL", "PRESCRIPTION"]
CHUNK_SIZE = 1_000_000


def export_table(conn: pyexasol.ExaConnection, table: str, output_dir: Path) -> None:
    full_name = f"{db.WAREHOUSE_SCHEMA}.{table}"
    count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
    print(f"{table}: {count:,} rows")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{table.lower()}.parquet"

    start = time.time()

    stmt = conn.execute(f"SELECT * FROM {full_name}")
    columns = [col["name"] for col in stmt.columns()]

    writer = None
    rows_written = 0

    while True:
        rows = stmt.fetchmany(CHUNK_SIZE)
        if not rows:
            break

        table_data = pa.table(
            {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        )

        if writer is None:
            writer = pq.ParquetWriter(str(output_file), table_data.schema)

        writer.write_table(table_data)
        rows_written += len(rows)
        print(f"  {rows_written:,} / {count:,} rows written")

    if writer is not None:
        writer.close()

    elapsed = time.time() - start
    size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"  Saved to {output_file} ({size_mb:.1f} MB) in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/parquet")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    conn = db.connect()
    db.ensure_schemas(conn)

    start = time.time()
    for table in TABLES:
        export_table(conn, table, output_dir)

    conn.close()
    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
