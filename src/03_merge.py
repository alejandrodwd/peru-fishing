import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "data/processed_validated"
OUTPUT_FILE = BASE_DIR / "data/processed_validated/peru_region_2024_full_validated.csv"

files = sorted(INPUT_DIR.glob("peru_region_2024-*_validated.csv"))

dfs = []

for file in files:
    
    if "full" in file.name: # skip full file if already exists
        continue

    df = pd.read_csv(file)
    dfs.append(df)          # collect months

# merge
full_df = pd.concat(dfs, ignore_index=True)

# sort for time series consistency
full_df = full_df.sort_values(by=["date", "cell_ll_lat", "cell_ll_lon"])

# checks
print("\n--- MERGE SUMMARY ---")
print("Total rows:", len(full_df))
print("Months included:", full_df["month"].nunique())
print("Date range:", full_df["date"].min(), "→", full_df["date"].max())

# save
full_df.to_csv(OUTPUT_FILE, index=False)

print(f"\nSaved full validated dataset to: {OUTPUT_FILE}")