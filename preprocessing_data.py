import os
import numpy as np
import pandas as pd
from glob import glob
from tqdm import tqdm

# Load full dataframe
full_df = pd.read_parquet("/scr/jmu/jump_feature/aggregated_results/wells.parquet")

# Base output directory
OUTPUT_BASE = "/scr/jmu/nc_batch/2023_Arevalo_NatComm_BatchCorrection-main/inputs/source_6/workspace/profiles"

# Group by Metadata_Plate
for plate, group in tqdm(full_df.groupby('Metadata_Plate'), desc="Writing plate-level parquet files"):
    # Extract Metadata_Batch (assumes all rows in group have same batch)
    batch = group['Metadata_Batch'].iloc[0]
    # Convert to string to avoid TypeErrors in path creation
    batch_str = str(batch)
    plate_str = str(plate)
    # Construct output path: .../<batch>/<plate>/<plate>.parquet
    output_path = os.path.join(OUTPUT_BASE, batch_str, plate_str)
    os.makedirs(output_path, exist_ok=True)
    # Drop Metadata_Batch from group
    group = group.drop(columns=["Metadata_Batch"])
    # Save parquet
    output_file = os.path.join(output_path, f"{plate_str}.parquet")
    group.to_parquet(output_file, index=False)

print("✅ All plate-level .parquet files written.")

feature_cols = [col for col in full_df.columns if col.startswith("feature")]
metadata_cols = ["Metadata_Source", "Metadata_Plate", "Metadata_Well", "Metadata_JCP2022"]

wells = (
    full_df
    .groupby(metadata_cols)[feature_cols]
    .mean()
    .reset_index()
) #94459

wells["Metadata_JCP2022"] = wells["Metadata_JCP2022"].apply(lambda x: MAPPER.get(x, x))
wells_clean = wells[~wells["Metadata_JCP2022"].isin(["UNTREATED", "UNKNOWN", "BAD CONSTRUCT"])] #89054
wells_clean.to_parquet("/scr/jmu/jump_feature/aggregated_results/wells.parquet")

