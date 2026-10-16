#!/usr/bin/env python3
"""Exercise CSV file rotation in an installed splunk-export runner."""

import argparse
import csv
import gzip
import importlib.util
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace


def load_runner(path: Path):
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("splunk_export_rotation_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load runner: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("runner", type=Path)
    args = parser.parse_args()
    runner = load_runner(args.runner.resolve())

    with tempfile.TemporaryDirectory(prefix="splunk-csv-rotation-") as temp_dir:
        output_dir = Path(temp_dir)
        runner.options = SimpleNamespace(
            output_destination="file",
            gzip="true",
            output_format="csv",
            max_file_size_mb="1",
            incremental_mode="false",
            incremental_time_source="file",
        )
        runner.global_vars = {"output_directory": str(output_dir)}
        runner.results.JSONResultsReader = lambda stream: iter(stream)

        payload = "x" * 20_000
        records = [
            {"_time": "2026-10-16 00:00:00.000 UTC", "_raw": payload, "sequence": i}
            for i in range(301)
        ]
        summary = runner.write_results_to_file(
            records,
            {"earliest": "2026-10-16T00:00:00", "index": "rotation-test"},
        )

        files = sorted(output_dir.rglob("*.csv.gz"))
        row_counts = []
        for path in files:
            with gzip.open(path, "rt", newline="") as handle:
                rows = list(csv.reader(handle))
            if not rows or rows[0] != ["_time", "_raw", "sequence"]:
                raise AssertionError(f"Missing or invalid CSV header in {path.name}")
            row_counts.append(len(rows) - 1)

        if summary["count"] != len(records):
            raise AssertionError(
                f"Runner stopped after {summary['count']} records instead of {len(records)}"
            )
        if sum(row_counts) != len(records):
            raise AssertionError(
                f"CSV segments contain {sum(row_counts)} records instead of {len(records)}"
            )
        if len(files) < 2:
            raise AssertionError("The test did not trigger CSV rotation")

        print(
            f"CSV rotation OK: {len(records)} records across {len(files)} files "
            f"({', '.join(map(str, row_counts))})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
