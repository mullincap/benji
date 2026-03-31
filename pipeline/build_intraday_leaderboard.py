#!/usr/bin/env python3
# ============================================================
# Intraday Leaderboard Builder — VECTORIZED
# Metric toggle: price / open_interest / volume
# ============================================================

import os
from pathlib import Path
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import argparse

# ============================================================
# CONFIG
# ============================================================

PARQUET_DIR = Path(
    os.environ.get("LEADERBOARD_PARQUET_DIR", "/Users/johnmullin/Desktop/desk/import/oi_logger/backfills/oi_raw/oi_parquet_partitioned")
)

OUTPUT_DIR = Path(
    os.environ.get("LEADERBOARD_OUTPUT_DIR", "/Users/johnmullin/Desktop/desk/import/oi_logger/backfills/oi_raw/intraday_leaderboards")
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_N = 333

# ============================================================
# OUTPUT SETTINGS
# ============================================================

SAVE_DAILY_FILES = False     # True = write per-day CSVs
BUILD_MASTER_FILE = True    # True = build merged master
MASTER_FILE_CSV = False
master_frames = []

# ============================================================
# CLI ARGUMENTS
# ============================================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--metric",
    type=str,
    required=True,
    help="Ranking metric: price | open_interest | volume"
)
parser.add_argument(
    "--parquet-path",
    type=str,
    default=None,
    dest="parquet_path",
    help=(
        "Path to a single master parquet file (e.g. master_oi_training_table.parquet). "
        "When supplied, the builder reads this file grouped by date instead of "
        "scanning PARQUET_DIR for date=* partitions. "
        "Required columns: timestamp_utc, symbol, <metric>."
    )
)
parser.add_argument(
    "--output-dir",
    type=str,
    default=None,
    dest="output_dir",
    help="Directory to write output parquet files. Overrides OUTPUT_DIR constant."
)

parser.add_argument(
    "--index-lookback",
    type=int,
    default=0,
    dest="index_lookback",
    help=(
        "Hours before deployment_start_hour to use as the % change anchor. "
        "Default: 0 = midnight (first bar of the day, current behaviour). "
        "e.g. --deployment-start-hour 6 --index-lookback 6 anchors at 00:00; "
        "     --deployment-start-hour 6 --index-lookback 4 anchors at 02:00; "
        "     --deployment-start-hour 6 --index-lookback 8 anchors at 22:00 prev day."
    )
)
parser.add_argument(
    "--deployment-start-hour",
    type=int,
    default=6,
    dest="deployment_start_hour",
    help="Deployment window start hour UTC (0-23). Used with --index-lookback to "
         "compute the anchor time: anchor = deployment_start_hour - index_lookback. "
         "Default: 6."
)

args = parser.parse_args()
RANK_METRIC = args.metric.lower()
if args.output_dir:
    OUTPUT_DIR = Path(args.output_dir)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# VALIDATE METRIC
# ============================================================

VALID_METRICS = ["price", "open_interest", "volume"]

if RANK_METRIC not in VALID_METRICS:
    raise ValueError(
        f"Invalid RANK_METRIC → {RANK_METRIC} | "
        f"Valid: {VALID_METRICS}"
    )

print(f"\n📊 Ranking Metric → {RANK_METRIC}")

# ── Anchor time ──────────────────────────────────────────────────────────
# anchor_offset = how many hours before deployment_start_hour the % change
# baseline is measured from. 0 = midnight (current behaviour).
INDEX_LOOKBACK       = args.index_lookback
DEPLOYMENT_START_HOUR = args.deployment_start_hour
_anchor_total         = DEPLOYMENT_START_HOUR - INDEX_LOOKBACK  # may be negative
ANCHOR_CROSS_MIDNIGHT = _anchor_total < 0
ANCHOR_HOUR           = _anchor_total % 24   # clock hour (wraps correctly)
ANCHOR_HHMM           = f"{ANCHOR_HOUR:02d}00"

if INDEX_LOOKBACK == 0:
    print(f"📍 Anchor → midnight (index_lookback=0, current behaviour)")
elif ANCHOR_CROSS_MIDNIGHT:
    print(f"📍 Anchor → {ANCHOR_HOUR:02d}:00 UTC previous day "
          f"(start={DEPLOYMENT_START_HOUR:02d}:00, lookback={INDEX_LOOKBACK}h — crosses midnight)")
else:
    print(f"📍 Anchor → {ANCHOR_HOUR:02d}:00 UTC same day "
          f"(start={DEPLOYMENT_START_HOUR:02d}:00, lookback={INDEX_LOOKBACK}h)")

# ============================================================
# START
# ============================================================

script_start = datetime.now()

print(f"\n🚀 Building intraday leaderboards")
print(f"Start → {script_start}\n")

# ── Determine data source ─────────────────────────────────────────────────
# Single-file mode: read entire parquet, group by date
# Partition mode:   scan PARQUET_DIR for date=* subdirectories (original)
USE_SINGLE_FILE = args.parquet_path is not None

if USE_SINGLE_FILE:
    print(f"📂 Single-file mode: {args.parquet_path}")
    _master_df = pd.read_parquet(
        args.parquet_path,
        columns=["timestamp_utc", "symbol", RANK_METRIC]
    )
    _master_df["timestamp_utc"] = pd.to_datetime(_master_df["timestamp_utc"])
    _master_df["_date"] = _master_df["timestamp_utc"].dt.date
    _date_groups = sorted(_master_df["_date"].unique())
    print(f"📂 Dates found: {len(_date_groups)}\n")
else:
    partitions = sorted(PARQUET_DIR.glob("date=*"))
    print(f"📂 Partitions found: {len(partitions)}\n")
    _date_groups = partitions

# ============================================================
# LOOP DATES
# ============================================================

for _day_key in tqdm(_date_groups, desc="Processing days"):

    try:

        if USE_SINGLE_FILE:
            import datetime as _dt
            date_str = str(_day_key)
            df = _master_df[_master_df["_date"] == _day_key][[
                "timestamp_utc", "symbol", RANK_METRIC
            ]].copy()
        else:
            date_str = _day_key.name.split("=")[1]
            df = pd.read_parquet(
                _day_key,
                columns=["timestamp_utc", "symbol", RANK_METRIC]
            )

        # Keep only required cols
        df = df[["timestamp_utc", "symbol", RANK_METRIC]]

        # ----------------------------------------------------
        # Minute normalization (collapse sub-minute prints)
        # ----------------------------------------------------

        # Bucket timestamps to the minute
        df["minute"] = df["timestamp_utc"].dt.floor("min")

        # Keep last observation per symbol per minute
        df = (
            df.sort_values("timestamp_utc")
              .groupby(["symbol", "minute"])
              .last()
              .reset_index()
        )

        # ----------------------------------------------------
        # Anchor-based % change
        # anchor = deployment_start_hour - index_lookback
        # ----------------------------------------------------

        if INDEX_LOOKBACK == 0:
            # Legacy midnight anchor: first bar of the day
            anchor_vals = (
                df.sort_values("minute")
                  .groupby("symbol")[RANK_METRIC]
                  .transform("first")
            )
        elif not ANCHOR_CROSS_MIDNIGHT:
            # Same-day anchor: find the bar closest to ANCHOR_HOUR:00
            anchor_ts = pd.Timestamp(date_str).replace(hour=ANCHOR_HOUR, minute=0)
            # Get last value at or before anchor_ts for each symbol
            anchor_df = (
                df[df["minute"] <= anchor_ts]
                  .sort_values("minute")
                  .groupby("symbol")[RANK_METRIC]
                  .last()
            )
            anchor_vals = df["symbol"].map(anchor_df)
        else:
            # Cross-midnight anchor: value lives in the previous day's partition
            prev_date_str = (
                pd.Timestamp(date_str) - pd.Timedelta(days=1)
            ).strftime("%Y-%m-%d")
            _prev_date_key = pd.Timestamp(prev_date_str).date()
            _prev_exists = (
                (USE_SINGLE_FILE and _prev_date_key in set(_master_df["_date"])) or
                (not USE_SINGLE_FILE and (PARQUET_DIR / f"date={prev_date_str}").exists())
            )
            if _prev_exists:
                if USE_SINGLE_FILE:
                    prev_df = _master_df[_master_df["_date"] == _prev_date_key][[
                        "timestamp_utc", "symbol", RANK_METRIC
                    ]].copy()
                else:
                    prev_df = pd.read_parquet(
                        PARQUET_DIR / f"date={prev_date_str}",
                        columns=["timestamp_utc", "symbol", RANK_METRIC]
                    )
                prev_df["minute"] = prev_df["timestamp_utc"].dt.floor("min")
                prev_df = (
                    prev_df.sort_values("timestamp_utc")
                           .groupby(["symbol", "minute"]).last().reset_index()
                )
                anchor_ts = (
                    pd.Timestamp(prev_date_str).replace(hour=ANCHOR_HOUR, minute=0)
                )
                prev_anchor = (
                    prev_df[prev_df["minute"] <= anchor_ts]
                           .sort_values("minute")
                           .groupby("symbol")[RANK_METRIC]
                           .last()
                )
                anchor_vals = df["symbol"].map(prev_anchor)
            else:
                # No previous partition — fall back to midnight of current day
                tqdm.write(f"  ⚠ No prev partition for {prev_date_str} "
                           f"— falling back to midnight anchor")
                anchor_vals = (
                    df.sort_values("minute")
                      .groupby("symbol")[RANK_METRIC]
                      .transform("first")
                )

        anchor_vals = anchor_vals.replace(0, pd.NA)

        df["pct_change_midnight"] = (
            df[RANK_METRIC] / anchor_vals - 1
        )

        # Clean bad anchors
        df = df.replace([float("inf"), -float("inf")], pd.NA)
        df = df.dropna(subset=["pct_change_midnight"])

        # ----------------------------------------------------
        # VECTOR RANKING
        # ----------------------------------------------------

        df["rank"] = (
            df.groupby("minute")["pct_change_midnight"]
              .rank(method="first", ascending=False)
              .astype("int16")
        )

        leaders = df[df["rank"] <= TOP_N]

        # ----------------------------------------------------
        # Pivot leaderboard
        # ----------------------------------------------------

        leaders = leaders.sort_values(
            ["minute", "rank"]
        )

        leaders["rank_col"] = (
            "R" + leaders["rank"].astype(int).astype(str)
        )

        out_df = (
            leaders
            .pivot(
                index="minute",
                columns="rank_col",
                values="symbol"
            )
            .reset_index()
        )

        # Rename minute → timestamp_utc
        out_df = out_df.rename(columns={"minute": "timestamp_utc"})

        # Sort columns R1 → R100
        out_df = out_df.reindex(
            sorted(out_df.columns, key=lambda x: (
                0 if x == "timestamp_utc" else int(x[1:])
            )),
            axis=1
        )

        rank_cols = [f"R{i}" for i in range(1, TOP_N+1)]

        for col in rank_cols:
            if col not in out_df.columns:
                out_df[col] = pd.NA

        # ----------------------------------------------------
        # Force full 1,440 minute index
        # ----------------------------------------------------

        day_start = pd.to_datetime(date_str)

        full_index = pd.date_range(
            start=day_start,
            periods=1440,
            freq="min"
        )

        out_df = (
            out_df
            .set_index("timestamp_utc")
            .reindex(full_index)
            .reset_index()
            .rename(columns={"index": "timestamp_utc"})
        )

        # ----------------------------------------------------
        # SAVE
        # ----------------------------------------------------

        if SAVE_DAILY_FILES:

            out_file = OUTPUT_DIR / (
                f"wide_intraday_pct_leaderboard_{RANK_METRIC}_top{TOP_N}_{date_str}.csv"
            )

            out_df.to_csv(out_file, index=False)

        if BUILD_MASTER_FILE:
            master_frames.append(out_df)

        tqdm.write(
            f"✔ {date_str} → "
            f"{df['symbol'].nunique()} symbols | "
            f"{len(out_df)} minutes"
        )

    except Exception as e:

        tqdm.write(f"❌ Failed → {part.name} | {e}")


# ============================================================
# BUILD MASTER FILE (ALL DAYS)
# ============================================================

if BUILD_MASTER_FILE:

    print("\n🧩 Building MASTER leaderboard file...")

    if not master_frames:
        print("⚠️ No dataframes accumulated.")
    else:

        master_df = (
            pd.concat(master_frames, ignore_index=True)
              .sort_values("timestamp_utc")
        )

        if MASTER_FILE_CSV:
            master_file = OUTPUT_DIR / (f"intraday_pct_leaderboard_{RANK_METRIC}_top{TOP_N}_anchor{ANCHOR_HHMM}_ALL.csv")
            master_df.to_csv(master_file, index=False)
        else:
            master_file = OUTPUT_DIR / (f"intraday_pct_leaderboard_{RANK_METRIC}_top{TOP_N}_anchor{ANCHOR_HHMM}_ALL.parquet")
            master_df.to_parquet(master_file, compression="snappy", row_group_size=500_000)

        print(
            f"\n✅ Master file built → {master_file.name}\n"
            f"Rows: {len(master_df):,}"
        )

# ============================================================
# DONE
# ============================================================

end_time = datetime.now()

print("\n✅ Leaderboard build complete")
print("Start :", script_start)
print("End   :", end_time)
print("Elapsed:", end_time - script_start)
