#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click"]
# ///
"""Parse coverage reports from Python and Rust test runs."""

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import click


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


@click.command()
@click.option("--python-xml", type=click.Path(path_type=Path), help="Path to Python coverage.xml")
@click.option("--rust-lcov", type=click.Path(path_type=Path), help="Path to Rust lcov.info")
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    help="Output JSON file (optional, prints to stdout if not specified)",
)
def main(python_xml: Path | None, rust_lcov: Path | None, output: Path | None) -> None:
    """Parse coverage reports from Python and Rust test runs."""
    result = {}

    if python_xml:
        result["python"] = parse_python_coverage(python_xml)

    if rust_lcov:
        result["rust"] = parse_rust_coverage(rust_lcov)

    output_str = json.dumps(result)
    if output:
        output.write_text(output_str)
    else:
        print(output_str)


if __name__ == "__main__":
    main()
