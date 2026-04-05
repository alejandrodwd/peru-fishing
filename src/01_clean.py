import pandas as pd
from pathlib import Path
import pycountry

dfs = []

# make dictionary of flag to country name
country_map = {c.alpha_3: c.name for c in pycountry.countries}

# define base project directory
BASE_DIR = Path(__file__).resolve().parents[1]

for i in range(12): # 12 months of year

    
    file_path = BASE_DIR / f"data/raw/fleet-monthly-csvs-10-v3-2024-{i+1:02d}-01.csv" # make file path
    print(file_path)
 
    df = pd.read_csv(file_path) # read file

    # cleaning
    df = df[(df["cell_ll_lat"].between(-18.5, -14.3)) & # latitude constraint
            (df["cell_ll_lon"].between(-80.7, -76))]    # longitude constraint
    df = df.drop(columns=["year"])                      # redundant column
    df["country_name"] = df["flag"].map(country_map)    # add country name column

    dfs.append(df) # store each month

    output_path = BASE_DIR / f"data/processed/peru_region_2024-{i+1:02d}.csv" # save cleaned data
    df.to_csv(output_path, index=False)

    print("done")
