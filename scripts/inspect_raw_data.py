import argparse
import csv
import os
from pathlib import Path

CONFIG = {
    "chicago": {"delimiter": ";", "encoding": "utf-8", "sample_rows": 3},
    "london": {"delimiter": ",", "encoding": "utf-8", "sample_rows": 3},
    "nyc": {"delimiter": ",", "encoding": "utf-8", "sample_rows": 3},
    "berlin": {"delimiter": ";", "encoding": "utf-8", "sample_rows": 3},
}


def inspect_file(source, file_path, cfg):
    print(f"\n{'='*60}")
    print(f"Source: {source}")
    print(f"File: {file_path}")
    print(f"Size: {os.path.getsize(file_path):,} bytes")
    try:
        with open(file_path, "r", encoding=cfg["encoding"], newline="") as f:
            reader = csv.reader(f, delimiter=cfg["delimiter"])
            header = next(reader)
            print(f"Columns ({len(header)}): {header}")
            rows = []
            for i, row in enumerate(reader):
                if i < cfg["sample_rows"]:
                    rows.append(row)
                else:
                    break
            for i, row in enumerate(rows, 1):
                display = row[:10] if len(row) > 10 else row
                print(f"Row {i}: {display}")

        year_cols = [c for c in header if c.lower() == "year"]
        print(f"Year columns: {year_cols}")

        time_cols = [c for c in header if "finish" in c.lower() or "time" in c.lower()]
        print(f"Time-related columns: {time_cols}")

        gender_col = next((c for c in header if c.lower() == "gender"), None)
        if gender_col:
            print(f"Gender column found: {gender_col}")
        else:
            print("No 'gender' column found")

    except Exception as e:
        print(f"Error reading {source}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Inspect raw marathon CSVs")
    parser.add_argument("--dir", default="data/raw", help="Path to raw data directory")
    args = parser.parse_args()
    raw_dir = Path(args.dir)

    if not raw_dir.exists():
        print(f"Directory {raw_dir} not found. Create it or use --dir.")
        return

    for source, cfg in CONFIG.items():
        files = sorted(raw_dir.glob(f"{source}*"))
        if not files:
            print(f"\nNo files found for {source}")
            continue
        for f in files:
            inspect_file(source, f, cfg)


if __name__ == "__main__":
    main()
