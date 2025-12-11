# Script to automatically download all required macro/financial time series from FRED (and a few related sources where applicable) and save them as CSVs.
# No FRED API key required – uses the public CSV download endpoint.

import os
import time
import requests
import pandas as pd


BASE_DIR = os.getcwd()      
DATA_DIR = os.path.join(BASE_DIR, "raw")

os.makedirs(DATA_DIR, exist_ok=True)


FRED_SERIES = {
    # --- Yield Curve: Treasuries ---
    "DGS3MO": "3-Month Treasury Constant Maturity Rate",
    "DGS1": "1-Year Treasury Constant Maturity Rate",
    "DGS2": "2-Year Treasury Constant Maturity Rate",
    "DGS5": "5-Year Treasury Constant Maturity Rate",
    "DGS7": "7-Year Treasury Constant Maturity Rate",
    "DGS10": "10-Year Treasury Constant Maturity Rate",
    "DGS20": "20-Year Treasury Constant Maturity Rate",
    "DGS30": "30-Year Treasury Constant Maturity Rate",

    # --- Recession Indicators ---
    "USREC": "US Recession Indicator (monthly)",
    "USRECM": "US Recession Indicator (alt monthly series)",

    # --- CPI (Inflation) ---
    "CPIAUCSL": "CPI All Urban Consumers (Headline)",
    "CPILFESL": "CPI All Urban Consumers: Core (Ex Food & Energy)",
    "CPIENGSL": "CPI Energy",
    "CPIFABSL": "CPI Food and Beverages",
    "CPIGODSL": "CPI Commodities (Goods)",
    "CPISRVSL": "CPI Services",

    # --- PCE Price Index (Fed’s preferred) ---
    "PCEPI": "PCE Price Index (Headline)",
    "PCEPILFE": "PCE Price Index (Core, Ex Food & Energy)",
    "PCEPIS": "PCE Services",
    "PCEPISDG": "PCE Durable Goods",

    # --- PPI (Producer Prices) ---
    "PPIFGS": "PPI: Final Demand",
    "PPIENG": "PPI: Energy",
    "PPICMM": "PPI: Commodities",

    # --- Supply/Demand Shock Proxies ---
    "DCOILWTICO": "WTI Crude Oil Spot Price",
    "PALLFNFINDEX": "Global Price Index of All Commodities",

    # NOTE: GSCPI is also on FRED now
    "GSCPI": "Global Supply Chain Pressure Index",

    # --- Unemployment: Headline ---
    "UNRATE": "Unemployment Rate (Total, 16+)",

    # --- Unemployment by Gender ---
    "LNS14000001": "Unemployment Rate - Men, 16+",
    "LNS14000002": "Unemployment Rate - Women, 16+",

    # --- Unemployment by Race ---
    "LNS14000003": "Unemployment Rate - White, 16+",
    "LNS14000006": "Unemployment Rate - Black or African American, 16+",
    "LNS14000009": "Unemployment Rate - Hispanic or Latino, 16+",

    # --- Unemployment by Age ---
    "LNS14000012": "Unemployment Rate - 16 to 19 years",
    "LNS14000089": "Unemployment Rate - 20 to 24 years",
    "LNS14000025": "Unemployment Rate - 25 to 54 years",
    "LNS14000036": "Unemployment Rate - 55 years and over",

    # --- Market-Based Inflation Expectations (TIPS) ---
    "T5YIE": "5-Year Breakeven Inflation Rate",
    "T10YIE": "10-Year Breakeven Inflation Rate",
    "T5YIFR": "5-Year, 5-Year Forward Inflation Expectation Rate",

    # --- Survey-Based Inflation Expectations ---
    "MICH": "University of Michigan: Inflation Expectation (12-month)",
    "MICH5YMV": "University of Michigan: Inflation Expectation (5-year)",

    # --- Wages / Labor Cost (optional but useful) ---
    "ECIALLCIV": "Employment Cost Index: Total Compensation for Civilians",
}


# ----------------------------
# 3. HELPER: DOWNLOAD FROM FRED
# ----------------------------

def download_fred_series(series_id: str, desc: str, out_dir: str = DATA_DIR,
                         sleep_seconds: float = 0.5) -> None:
    """
    Download a single FRED series as CSV using the public endpoint.

    Saves to: data/raw/<series_id>.csv
    """
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    out_path = os.path.join(out_dir, f"{series_id}.csv")

    print(f"Downloading {series_id} - {desc} ...")

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        # Save raw CSV as returned by FRED
        with open(out_path, "wb") as f:
            f.write(r.content)

        # Optional: sanity check using pandas, and re-save as clean CSV
        df = pd.read_csv(out_path)
        # Standardize column names: DATE, VALUE -> date, value
        df.columns = [c.lower() for c in df.columns]
        # Convert date column to datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.to_csv(out_path, index=False)

        print(f"✔ Saved to {out_path} (rows: {len(df)})")

    except Exception as e:
        print(f"✖ Failed to download {series_id}: {e}")

    # Be polite to FRED servers
    time.sleep(sleep_seconds)


# ----------------------------
# 4. MAIN: LOOP OVER ALL SERIES
# ----------------------------

def main():
    print(f"Saving data into: {DATA_DIR}")
    print(f"Total FRED series to download: {len(FRED_SERIES)}")
    print("-" * 60)

    for sid, desc in FRED_SERIES.items():
        download_fred_series(sid, desc)

    print("-" * 60)
    print("Finished downloading all series.")
    print("You can now load them from data/raw/*.csv in your EDAV notebook.")


if __name__ == "__main__":
    main()