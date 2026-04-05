import pandas as pd
from pathlib import Path
import pycountry

BASE_DIR = Path(__file__).resolve().parents[1]

# box
LAT_MIN, LAT_MAX = -18.5, -14.3
LON_MIN, LON_MAX = -80.7, -76
    
for i in range(12):
    INPUT_FILE = BASE_DIR / f"data/processed/peru_region_2024-{i+1:02d}.csv" 
    OUTPUT_FILE = BASE_DIR / f"data/processed_validated/peru_region_2024-{i+1:02d}_validated.csv"

    # read file
    df = pd.read_csv(INPUT_FILE)

    # basic type fixes
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["cell_ll_lat"] = pd.to_numeric(df["cell_ll_lat"], errors="coerce")
    df["cell_ll_lon"] = pd.to_numeric(df["cell_ll_lon"], errors="coerce")
    df["hours"] = pd.to_numeric(df["hours"], errors="coerce")
    df["fishing_hours"] = pd.to_numeric(df["fishing_hours"], errors="coerce")
    df["mmsi_present"] = pd.to_numeric(df["mmsi_present"], errors="coerce")

    # country validation
    country_map = {c.alpha_3: c.name for c in pycountry.countries}
    valid_flags = set(country_map.keys())

    # flag validity check
    df["flag_valid"] = df["flag"].isin(valid_flags)

    # map country names safely
    df["country_name"] = df["flag"].map(country_map)

    # handle missing mappings
    df["country_name"] = df["country_name"].fillna("Unknown")

    # spatial validation
    df["valid_coords"] = (
        df["cell_ll_lat"].between(LAT_MIN, LAT_MAX) &
        df["cell_ll_lon"].between(LON_MIN, LON_MAX)
    )

    # missing value checks
    df["missing_country"] = df["country_name"].isna()
    df["missing_fishing_hours"] = df["fishing_hours"].isna()

    # duplicate check
    df["is_duplicate"] = df.duplicated()

    # valid hours
    df["valid_hours"] = df["hours"] >= 0
    df["valid_fishing_hours"] = df["fishing_hours"] >= 0

    # summary print
    print("\nMONTH:", i+1)
    print("--- VALIDATION SUMMARY ---")
    print("Total rows:", len(df))
    print("Invalid flags:", (~df["flag_valid"]).sum())
    print("Missing country names:", df["country_name"].isna().sum())
    print("Invalid coordinates:", (~df["valid_coords"]).sum())
    print("Duplicates:", df["is_duplicate"].sum())

    # save validated data
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved validated data to: {OUTPUT_FILE}")





