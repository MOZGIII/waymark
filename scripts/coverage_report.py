#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///
"""Parse coverage reports from Python and Rust test runs."""

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_python_coverage(xml_path: Path) -> dict:
    """Parse Python coverage from Cobertura XML format."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    line_rate = float(root.get("line-rate", 0)) * 100
    branch_rate = float(root.get("branch-rate", 0)) * 100

    return {
        "line": round(line_rate, 1),
        "branch": round(branch_rate, 1),
    }


def parse_rust_coverage(lcov_path: Path) -> dict:
    """Parse Rust coverage from LCOV format."""
    lines_hit = 0
    lines_total = 0
    branches_hit = 0
    branches_total = 0

    with open(lcov_path) as f:
        for line in f:
            if line.startswith("LH:"):
                lines_hit += int(line.split(":")[1])
            elif line.startswith("LF:"):
                lines_total += int(line.split(":")[1])
            elif line.startswith("BRH:"):
                branches_hit += int(line.split(":")[1])
            elif line.startswith("BRF:"):
                branches_total += int(line.split(":")[1])

    line_pct = (lines_hit / lines_total * 100) if lines_total > 0 else 0
    branch_pct = (branches_hit / branches_total * 100) if branches_total > 0 else None

    return {
        "line": round(line_pct, 1),
        "branch": round(branch_pct, 1) if branch_pct is not None else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse coverage reports")
    parser.add_argument("--python-xml", type=Path, help="Path to Python coverage.xml")
    parser.add_argument("--rust-lcov", type=Path, help="Path to Rust lcov.info")
    parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON file (optional, prints to stdout if not specified)",
    )

    args = parser.parse_args()

    result = {}

    if args.python_xml:
        result["python"] = parse_python_coverage(args.python_xml)

    if args.rust_lcov:
        result["rust"] = parse_rust_coverage(args.rust_lcov)

    output = json.dumps(result)
    if args.output:
        args.output.write_text(output)
    else:
        print(output)


if __name__ == "__main__":
    main()
