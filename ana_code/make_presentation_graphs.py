"""
This script generates presentation graphs comparing summer and winter Broadway show statistics over time, using the comparison tables created by the Spark job in make_comparison_tables.py. 
It reads the CSV outputs from HDFS, processes the data, and creates line plots for attendance, capacity, and gross revenue for both the full dataset and the dataset excluding major blockbusters. 
The resulting PNG files are saved locally for use in presentations or reports.

Outputs:
- 1990_2016_attendance.png
- 1990_2016_capacity.png
- 1990_2016_gross.png
- 2016_2026_attendance.png
- 2016_2026_capacity.png
- 2016_2026_gross.png
- 1990_2016_blockbuster_compare_attendance.png
- 1990_2016_blockbuster_compare_capacity.png
- 1990_2016_blockbuster_compare_gross.png

Assumes these HDFS outputs already exist:
- /user/<USER>/comparison_table_all
- /user/<USER>/comparison_table_no_big
"""

from __future__ import annotations

import argparse
import subprocess
from io import StringIO
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def read_hdfs_csv_dir(hdfs_dir: str) -> pd.DataFrame:
    """Read all CSV part files from an HDFS output directory into a DataFrame."""
    cmd = f"hdfs dfs -cat {hdfs_dir}/part-*"
    result = subprocess.run(
        cmd,
        shell=True,
        check=True,
        capture_output=True,
        text=True,
    )
    return pd.read_csv(StringIO(result.stdout))


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names and types."""
    rename_map = {
        "summer_ATT": "summer_att",
        "winter_ATT": "winter_att",
        "summer_CAP": "summer_cap",
        "winter_CAP": "winter_cap",
        "summer_GROSS": "summer_gross",
        "winter_GROSS": "winter_gross",
    }
    df = df.rename(columns=rename_map).copy()
    df["year"] = df["year"].astype(int)
    return df


def make_two_season_graph(
    df: pd.DataFrame,
    years: tuple[int, int],
    dataset_name: str,
    metric: str,
    outpath: Path,
    title: str,
) -> None:
    """Plot summer vs winter for one metric."""
    start_year, end_year = years
    filtered = df[
        (df["dataset_name"] == dataset_name)
        & (df["year"] >= start_year)
        & (df["year"] <= end_year)
    ].sort_values("year")

    plt.figure(figsize=(11, 6))
    plt.plot(filtered["year"], filtered[f"summer_{metric}"], marker="o", label="Summer")
    plt.plot(filtered["year"], filtered[f"winter_{metric}"], marker="o", label="Winter")
    plt.xlabel("Year")
    plt.ylabel(metric.replace("_", " ").title())
    plt.title(title)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def make_blockbuster_compare_graph(
    all_df: pd.DataFrame,
    no_big_df: pd.DataFrame,
    years: tuple[int, int],
    metric: str,
    outpath: Path,
    title: str,
) -> None:
    """Plot 1990–2016 with and without blockbuster shows for one metric."""
    start_year, end_year = years

    all_filtered = all_df[
        (all_df["dataset_name"] == "full")
        & (all_df["year"] >= start_year)
        & (all_df["year"] <= end_year)
    ].sort_values("year")

    no_big_filtered = no_big_df[
        (no_big_df["dataset_name"] == "full")
        & (no_big_df["year"] >= start_year)
        & (no_big_df["year"] <= end_year)
    ].sort_values("year")

    plt.figure(figsize=(12, 6))
    plt.plot(
        all_filtered["year"],
        all_filtered[f"summer_{metric}"],
        marker="o",
        label="Summer (All Shows)",
    )
    plt.plot(
        all_filtered["year"],
        all_filtered[f"winter_{metric}"],
        marker="o",
        label="Winter (All Shows)",
    )
    plt.plot(
        no_big_filtered["year"],
        no_big_filtered[f"summer_{metric}"],
        marker="o",
        linestyle="--",
        label="Summer (No Big Shows)",
    )
    plt.plot(
        no_big_filtered["year"],
        no_big_filtered[f"winter_{metric}"],
        marker="o",
        linestyle="--",
        label="Winter (No Big Shows)",
    )

    plt.xlabel("Year")
    plt.ylabel(metric.replace("_", " ").title())
    plt.title(title)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", required=True, help="HDFS username, e.g. lb4140_nyu_edu")
    parser.add_argument(
        "--outdir",
        default="presentation_graphs",
        help="Local output directory for PNG files",
    )
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    all_hdfs = f"/user/{args.user}/comparison_table_all"
    no_big_hdfs = f"/user/{args.user}/comparison_table_no_big"

    all_df = normalize_columns(read_hdfs_csv_dir(all_hdfs))
    no_big_df = normalize_columns(read_hdfs_csv_dir(no_big_hdfs))

    # 1990–2016, all shows
    for metric in ["att", "cap", "gross"]:
        make_two_season_graph(
            df=all_df,
            years=(1990, 2016),
            dataset_name="full",
            metric=metric,
            outpath=outdir / f"1990_2016_{metric}.png",
            title=f"1990–2016 Full Dataset: Summer vs Winter {metric.upper()}",
        )

    # 2016–2026, recent dataset
    for metric in ["att", "cap", "gross"]:
        make_two_season_graph(
            df=all_df,
            years=(2016, 2026),
            dataset_name="recent",
            metric=metric,
            outpath=outdir / f"2016_2026_{metric}.png",
            title=f"2016–2026 Recent Dataset: Summer vs Winter {metric.upper()}",
        )

    # 1990–2016, compare all shows vs no big shows
    for metric in ["att", "cap", "gross"]:
        make_blockbuster_compare_graph(
            all_df=all_df,
            no_big_df=no_big_df,
            years=(1990, 2016),
            metric=metric,
            outpath=outdir / f"1990_2016_blockbuster_compare_{metric}.png",
            title=f"1990–2016 Full Dataset: {metric.upper()} With vs Without Blockbusters",
        )

    print(f"Saved graphs to: {outdir.resolve()}")


if __name__ == "__main__":
    main()