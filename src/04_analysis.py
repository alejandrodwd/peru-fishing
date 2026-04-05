import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

# setup
BASE_DIR = Path(__file__).resolve().parents[1]

for i in range(12):
    INPUT_FILE = BASE_DIR / f"data/processed_validated/peru_region_2024-{i+1:02d}_validated.csv"
    EEZ_FILE = BASE_DIR / "data/eez/eez.shp"
    OUTPUT_DIR = BASE_DIR / f"data/final"

    df = pd.read_csv(INPUT_FILE)

    # convert to GeoDataFrame
    geometry = [Point(xy) for xy in zip(df["cell_ll_lon"], df["cell_ll_lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # load EEZ, convert to WGS84
    eez = gpd.read_file(EEZ_FILE)
    eez = eez.to_crs("EPSG:4326")

    # new col: inside_eez (T/F)
    gdf["inside_eez"] = gdf.within(eez.union_all())

    # 1. Monthly fishing hours per country, split by inside/outside EEZ
    month_country = gdf.groupby(["inside_eez", "country_name"])["fishing_hours"].sum().reset_index()
    month_country = month_country.sort_values("fishing_hours", ascending=False)
    month_country["fishing_hours"] = month_country["fishing_hours"].round(1)
    month_country.to_csv(OUTPUT_DIR / f"month_{i+1:02d}_country_eez.csv", index=False)
    
    # 2. Monthly fishing hours inside/outside EEZ
    month_total = gdf.groupby("inside_eez")["fishing_hours"].sum().reset_index()
    month_total["fishing_hours"] = month_total["fishing_hours"].round(1)
    month_total.to_csv(OUTPUT_DIR / f"month_{i+1:02d}_eez.csv", index=False)



# ----------------------------------------------------------
# yearly sets
# ----------------------------------------------------------

INPUT_FILE = BASE_DIR / "data/processed_validated/peru_region_2024_full_validated.csv"
EEZ_FILE = BASE_DIR / "data/eez/eez.shp"
OUTPUT_DIR = BASE_DIR / f"data/final"

df = pd.read_csv(INPUT_FILE)

# convert to GeoDataFrame
geometry = [Point(xy) for xy in zip(df["cell_ll_lon"], df["cell_ll_lat"])]
gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

# load EEZ, convert to WGS84
eez = gpd.read_file(EEZ_FILE)
eez = eez.to_crs("EPSG:4326")

# new col: inside_eez (T/F)
gdf["inside_eez"] = gdf.within(eez.union_all())


# 1. Yearly fishing hours per country, split by inside/outside EEZ
year_country = gdf.groupby(
    ["inside_eez", "country_name"]
)["fishing_hours"].sum().reset_index()
year_country = year_country.sort_values("fishing_hours", ascending=False)
year_country["fishing_hours"] = year_country["fishing_hours"].round(1)
year_country.to_csv(OUTPUT_DIR / "year_country_eez.csv", index=False)

# 2. Yearly fishing hours inside/outside EEZ
year_total = gdf.groupby("inside_eez")["fishing_hours"].sum().reset_index()
year_total["fishing_hours"] = year_total["fishing_hours"].round(1)
year_total.to_csv(OUTPUT_DIR / "year_eez.csv", index=False)

# 3. Yearly gear type split by inside/outside EEZ
gear_total = gdf.groupby(
    ["inside_eez", "geartype"]
)["fishing_hours"].sum().reset_index()
gear_total = gear_total.sort_values("fishing_hours", ascending=False)
gear_total["fishing_hours"] = gear_total["fishing_hours"].round(1)
gear_total.to_csv(OUTPUT_DIR / "year_gear_eez.csv", index=False)

# 4. Yearly fishing contribution by country, split by inside/outside EEZ 
year_country["percentage"] = (year_country.groupby("inside_eez")["fishing_hours"].transform(lambda x: x / x.sum()))
year_country["fishing_hours"] = year_country["fishing_hours"].round(1)
year_country["percentage"] = year_country["percentage"].round(3)
year_country.to_csv(OUTPUT_DIR / "year_country_percentage_eez.csv", index=False)

# final print
print("\n--- ANALYSIS COMPLETE ---")
print(f"Saved outputs to: {OUTPUT_DIR}")

