#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

MAX_EXCEL_ROWS = 1_048_576


def log_step(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert all CSV files from a folder into Excel workbooks."
    )
    parser.add_argument(
        "--input-dir",
        default="data_synth/raw",
        help="Folder containing the source CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Folder where .xlsx files will be written. Defaults to the input folder.",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV encoding passed to pandas.read_csv().",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter passed to pandas.read_csv().",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Excel files.",
    )
    return parser.parse_args()


def convert_file(
    csv_path: Path,
    output_dir: Path,
    *,
    encoding: str,
    delimiter: str,
    overwrite: bool,
) -> Path | None:
    excel_path = output_dir / f"{csv_path.stem}.xlsx"
    if excel_path.exists() and not overwrite:
        log_step(f"Skip {excel_path.name}: file already exists")
        return None

    df = pd.read_csv(csv_path, encoding=encoding, sep=delimiter)
    max_data_rows = MAX_EXCEL_ROWS - 1

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        if len(df) <= max_data_rows:
            df.to_excel(writer, sheet_name="Sheet1", index=False)
        else:
            for index, start in enumerate(range(0, len(df), max_data_rows), start=1):
                chunk = df.iloc[start : start + max_data_rows]
                chunk.to_excel(writer, sheet_name=f"Sheet{index}", index=False)

    log_step(f"Converted {csv_path.name} -> {excel_path.name}")
    return excel_path


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else input_dir

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    if not input_dir.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")

    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        log_step(f"No CSV files found in {input_dir}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    log_step(f"Start CSV -> Excel conversion from {input_dir} to {output_dir}")

    converted = 0
    skipped = 0
    for csv_file in csv_files:
        result = convert_file(
            csv_file,
            output_dir,
            encoding=args.encoding,
            delimiter=args.delimiter,
            overwrite=args.overwrite,
        )
        if result is None:
            skipped += 1
        else:
            converted += 1

    log_step(f"Completed: converted={converted}, skipped={skipped}, total={len(csv_files)}")


if __name__ == "__main__":
    main()
