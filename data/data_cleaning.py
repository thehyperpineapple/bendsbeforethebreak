import os
import pandas as pd
import numpy as np

RAW_DIR = "raw"
CLEAN_DIR = "clean"
os.makedirs(CLEAN_DIR, exist_ok=True)

# Display settings
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", None)

def load_and_clean_fred_series(path):
    """
    Load a FRED CSV and return a DataFrame with:
      - 'date' column (parsed as datetime)
      - one value column named after the series ID

    It assumes:
      - first column = date-like
      - second column = values
    """
    series_id = os.path.basename(path).split(".")[0]

    df = pd.read_csv(path)

    # Minimal sanity check
    if df.shape[1] < 2:
        raise ValueError(f"Unexpected format in {path}: need at least 2 columns, got {df.shape[1]}")

    # Take first two columns regardless of their names
    date_col = df.columns[0]
    value_col = df.columns[1]

    df = df[[date_col, value_col]].copy()

    # Rename to standard names
    df.rename(columns={date_col: "date", value_col: series_id}, inplace=True)

    # Parse dates & values
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[series_id] = pd.to_numeric(df[series_id], errors="coerce")

    # Drop rows where date is missing
    df = df.dropna(subset=["date"])

    return df[["date", series_id]]

all_series = {}

for filename in os.listdir(RAW_DIR):
    if filename.endswith(".csv"):
        sid = filename.replace(".csv", "")
        path = os.path.join(RAW_DIR, filename)
        try:
            df = load_and_clean_fred_series(path)
            all_series[sid] = df
            print("Loaded:", sid, "shape:", df.shape)
        except Exception as e:
            print("Error loading", sid, ":", e)

master_df = None

for sid, df in all_series.items():
    if master_df is None:
        master_df = df
    else:
        master_df = master_df.merge(df, on="date", how="outer")

master_df = master_df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
print("Merged shape:", master_df.shape)

# Use end-of-month values for all series
master_df = master_df.set_index("date").resample("M").last()
master_df.index.name = "date"

print("After monthly resampling:", master_df.shape)


master_df.notna().any(axis=1).idxmax()   # first date where at least one series has data
master_df = master_df.loc["1954-01-31":]
threshold = 0.95
master_df = master_df.loc[:, master_df.isna().mean() < threshold]
yield_cols = [c for c in master_df.columns if c.startswith("DGS")]
master_df[yield_cols] = master_df[yield_cols].ffill()
macro_cols = [c for c in master_df.columns 
              if c not in yield_cols and c != "USREC"]
master_df[macro_cols] = master_df[macro_cols].ffill()
if "USREC" in master_df.columns:
    master_df["USREC"] = master_df["USREC"].fillna(0).astype(int)
master_df = master_df.dropna(how="all")

master_path = os.path.join(CLEAN_DIR, "master_df.csv")
master_df.to_csv(master_path)
print("Saved cleaned dataset to:", master_path)
