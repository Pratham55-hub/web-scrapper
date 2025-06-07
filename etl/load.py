# etl/load.py

import pandas as pd
import os
from typing import Optional, Dict

def load(df: pd.DataFrame, config: Optional[Dict] = None):
    """
    Loads (saves) the DataFrame to the output location/format specified in config.
    Supports Parquet (default) and CSV. Can be extended to DB, Excel, etc.
    """
    config = config or {}
    output_dir = config.get("output_dir", "data")
    site = config.get("site", "output")
    output_format = config.get("output_format", "parquet").lower()
    filename = config.get("output_filename", f"{site}_final.{output_format}")
    output_path = os.path.join(output_dir, filename)

    os.makedirs(output_dir, exist_ok=True)

    if output_format == "parquet":
        df.to_parquet(output_path, index=False)
        print(f"[LOAD] Data saved to {output_path} (Parquet format)")
    elif output_format == "csv":
        df.to_csv(output_path, index=False)
        print(f"[LOAD] Data saved to {output_path} (CSV format)")
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    # Optionally return the path for downstream processes
    return output_path
