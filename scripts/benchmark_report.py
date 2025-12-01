#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click"]
# ///
"""Run benchmarks and generate comparison reports."""

import json
from pathlib import Path

import click


def format_change(current: float, baseline: float, higher_is_better: bool = True) -> str:
    """Format a change as a percentage with emoji indicator."""
    if baseline == 0:
        return "N/A"

    pct_change = ((current - baseline) / baseline) * 100

    if higher_is_better:
        if pct_change > 5:
            emoji = ":rocket:"
        elif pct_change < -5:
            emoji = ":warning:"
        else:
            emoji = ":white_check_mark:"
    else:
        # Lower is better (e.g., latency)
        if pct_change < -5:
            emoji = ":rocket:"
        elif pct_change > 5:
            emoji = ":warning:"
        else:
            emoji = ":white_check_mark:"

    sign = "+" if pct_change > 0 else ""
    return f"{sign}{pct_change:.1f}% {emoji}"


def generate_markdown_report(
    pr_results: dict,
    main_results: dict,
    benchmark_diff: str,
    pr_sha: str,
    main_sha: str,
) -> str:
    """Generate a markdown comparison report."""
    lines = [
        "## :bar_chart: Benchmark Comparison",
        "",
        f"Comparing `{pr_sha[:8]}` (PR) vs `{main_sha[:8]}` (main)",
        "",
    ]

    # Check if benchmark code changed
    if benchmark_diff.strip():
        lines.extend(
            [
                "<details>",
                "<summary>:warning: <strong>Benchmark code changed in this PR</strong></summary>",
                "",
                "```diff",
                benchmark_diff[:3000],  # Truncate if too long
                "```",
                "",
                "</details>",
                "",
            ]
        )

    # Results table
    lines.extend(
        [
            "### Results",
            "",
            "| Benchmark | Metric | PR | Main | Change |",
            "|-----------|--------|---:|-----:|-------:|",
        ]
    )

    for bench_name in sorted(set(pr_results.keys()) | set(main_results.keys())):
        pr_bench = pr_results.get(bench_name, {})
        main_bench = main_results.get(bench_name, {})

        # Throughput row
        pr_tp = pr_bench.get("throughput", 0)
        main_tp = main_bench.get("throughput", 0)
        change_tp = format_change(pr_tp, main_tp, higher_is_better=True) if main_tp else "N/A"
        lines.append(
            f"| **{bench_name}** | Throughput (msg/s) | {pr_tp:.0f} | {main_tp:.0f} | {change_tp} |"
        )

        # P95 latency row
        pr_p95 = pr_bench.get("p95_ms", 0)
        main_p95 = main_bench.get("p95_ms", 0)
        if pr_p95 and main_p95:
            change_p95 = format_change(pr_p95, main_p95, higher_is_better=False)
            lines.append(f"| | P95 Latency (ms) | {pr_p95:.1f} | {main_p95:.1f} | {change_p95} |")

    lines.extend(
        [
            "",
            "<details>",
            "<summary>Raw benchmark data</summary>",
            "",
            "**PR Results:**",
            "```json",
            json.dumps(pr_results, indent=2),
            "```",
            "",
            "**Main Results:**",
            "```json",
            json.dumps(main_results, indent=2),
            "```",
            "",
            "</details>",
        ]
    )

    return "\n".join(lines)


@click.command()
@click.option("--pr-json", type=click.Path(exists=True), help="Path to PR benchmark results JSON")
@click.option(
    "--main-json", type=click.Path(exists=True), help="Path to main benchmark results JSON"
)
@click.option(
    "--benchmark-diff", type=click.Path(exists=True), help="Path to benchmark code diff file"
)
@click.option("--pr-sha", default="unknown", help="PR commit SHA")
@click.option("--main-sha", default="unknown", help="Main commit SHA")
@click.option("--output", "-o", type=click.Path(), help="Output file path")
@click.option(
    "--format", "output_format", type=click.Choice(["json", "markdown"]), default="markdown"
)
def main(
    pr_json: str | None,
    main_json: str | None,
    benchmark_diff: str | None,
    pr_sha: str,
    main_sha: str,
    output: str | None,
    output_format: str,
):
    """Generate benchmark comparison report."""
    # Load results
    pr_results = {}
    main_results = {}
    diff_content = ""

    if pr_json:
        with open(pr_json) as f:
            pr_results = json.load(f)

    if main_json:
        with open(main_json) as f:
            main_results = json.load(f)

    if benchmark_diff:
        with open(benchmark_diff) as f:
            diff_content = f.read()

    if output_format == "json":
        result = {
            "pr": pr_results,
            "main": main_results,
            "pr_sha": pr_sha,
            "main_sha": main_sha,
        }
        content = json.dumps(result, indent=2)
    else:
        content = generate_markdown_report(pr_results, main_results, diff_content, pr_sha, main_sha)

    if output:
        Path(output).write_text(content)
    else:
        print(content)


if __name__ == "__main__":
    main()
