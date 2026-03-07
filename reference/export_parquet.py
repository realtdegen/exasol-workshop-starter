"""
Export warehouse tables to Parquet files.

Streams data from Exasol via pyexasol HTTP transport (CSV),
reads in chunks with PyArrow, and writes chunked Parquet files.

Usage:
    uv run python export_parquet.py [--output-dir data/parquet]
"""

import argparse
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

import utils.db as db


CHUNK_SIZE = 10_000_000

SCHEMAS = {
    "PRACTICE": pa.schema([
        ("PRACTICE_CODE", pa.string()),
        ("PRACTICE_NAME", pa.string()),
        ("ADDRESS", pa.string()),
        ("COUNTY", pa.string()),
        ("POSTCODE", pa.string()),
        ("PERIOD", pa.string()),
    ]),
    "CHEMICAL": pa.schema([
        ("CHEMICAL_CODE", pa.string()),
        ("CHEMICAL_NAME", pa.string()),
        ("PERIOD", pa.string()),
    ]),
    "PRESCRIPTION": pa.schema([
        ("PRACTICE_CODE", pa.string()),
        ("BNF_CODE", pa.string()),
        ("DRUG_NAME", pa.string()),
        ("ITEMS", pa.int64()),
        ("NET_COST", pa.decimal128(18, 2)),
        ("ACTUAL_COST", pa.decimal128(18, 2)),
        ("QUANTITY", pa.int64()),
        ("PERIOD", pa.string()),
    ]),
}


def export_callback(pipe, dst, **kwargs):
    """Callback for export_to_callback: reads CSV stream, writes chunked Parquet."""
    schema = kwargs["schema"]
    table_dir = kwargs["table_dir"]
    table_name = kwargs["table_name"]
    count = kwargs["count"]

    convert_options = pcsv.ConvertOptions(column_types=schema)
    read_options = pcsv.ReadOptions(block_size=64 * 1024 * 1024)
    parse_options = pcsv.ParseOptions(newlines_in_values=True)
    reader = pcsv.open_csv(pipe, convert_options=convert_options,
                           read_options=read_options,
                           parse_options=parse_options)

    chunk_num = 0
    rows_written = 0
    batch_buf = []
    batch_rows = 0

    for batch in reader:
        batch_buf.append(batch)
        batch_rows += len(batch)

        if batch_rows >= CHUNK_SIZE:
            chunk_num += 1
            chunk_table = pa.Table.from_batches(batch_buf, schema=schema)
            parquet_path = table_dir / f"{table_name}-{chunk_num:04d}.parquet"
            pq.write_table(chunk_table, str(parquet_path))
            rows_written += batch_rows
            size_mb = parquet_path.stat().st_size / (1024 * 1024)
            pct = rows_written / count * 100
            print(f"\r  {rows_written:,} / {count:,} ({pct:.0f}%) — {parquet_path.name} ({size_mb:.0f} MB)  ",
                  end="", flush=True)
            batch_buf = []
            batch_rows = 0

    if batch_buf:
        chunk_num += 1
        chunk_table = pa.Table.from_batches(batch_buf, schema=schema)
        parquet_path = table_dir / f"{table_name}-{chunk_num:04d}.parquet"
        pq.write_table(chunk_table, str(parquet_path))
        rows_written += batch_rows
        size_mb = parquet_path.stat().st_size / (1024 * 1024)
        pct = rows_written / count * 100
        print(f"\r  {rows_written:,} / {count:,} ({pct:.0f}%) — {parquet_path.name} ({size_mb:.0f} MB)  ",
              end="", flush=True)

    print(f"\n  {chunk_num} file(s)")


def export_table(conn, table: str, output_dir: Path) -> None:
    full_name = f"{db.WAREHOUSE_SCHEMA}.{table}"
    schema = SCHEMAS[table]
    count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]

    table_dir = output_dir / table.lower()
    table_dir.mkdir(parents=True, exist_ok=True)

    table_name = table.lower()
    print(f"{table}: {count:,} rows")
    t0 = time.time()

    conn.export_to_callback(
        export_callback, None,
        f"SELECT * FROM {full_name}",
        export_params={"with_column_names": True},
        callback_params={
            "schema": schema,
            "table_dir": table_dir,
            "table_name": table_name,
            "count": count,
        },
    )

    print(f"  Done in {time.time() - t0:.1f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/parquet")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    conn = db.connect()
    db.ensure_schemas(conn)

    start = time.time()
    for table in SCHEMAS:
        export_table(conn, table, output_dir)

    conn.close()
    print(f"\nTotal: {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
