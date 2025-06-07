import pandas as pd
import numpy as np
import re
from typing import Optional, Dict

def transform(df: pd.DataFrame, config: Optional[Dict] = None) -> pd.DataFrame:
    """
    Cleans, standardizes, and enriches the scraped DataFrame.
    Config can be used for custom tweaks per site/category (optional).
    """
    # 1. Remove duplicates (prefer Product Link, else Product Name)
    col_link = 'Product Link'
    col_name = 'Product Name'
    if col_link in df.columns:
        df = df.drop_duplicates(subset=[col_link])
    elif col_name in df.columns:
        df = df.drop_duplicates(subset=[col_name])
    else:
        df = df.drop_duplicates()

    # 2. Standardize column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # 3. Trim whitespace from all string columns
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

    # 4. Fill missing company_name/brand with 'Unknown'
    for col in ['company_name', 'brand']:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown')

    # --- LOCATION TRANSFORMATION START ---
    # If you have a column called 'seller_city_state' (e.g. 'Surat, Gujarat'), split it:
    if 'seller_city' not in df.columns and 'seller_state' not in df.columns:
        # Try to split a generic 'location' or similar column, or a single seller_location column
        possible_location_columns = [c for c in df.columns if 'Seller_Address' in c]
        for col in possible_location_columns:
            # Only split if not already split
            if col in df.columns and col not in ['seller_city', 'seller_state']:
                city_state_split = df[col].str.split(',', n=1, expand=True)
                if city_state_split.shape[1] == 2:
                    df['seller_city'] = city_state_split[0].str.strip()
                    df['seller_state'] = city_state_split[1].str.strip()
    # If 'seller_city'/'seller_state' are present, just standardize them
    for loc_col in ['seller_city', 'seller_state']:
        if loc_col in df.columns:
            df[loc_col] = df[loc_col].fillna('Unknown').str.title().str.strip()

    # Clean/standardize seller address if present
    if 'seller_address' in df.columns:
        df['seller_address'] = (
            df['seller_address']
            .fillna('Unknown')
            .str.replace('\s+', ' ', regex=True)
            .str.strip()
        )
    # --- LOCATION TRANSFORMATION END ---

    # 5. Data type conversion (try numerics except 'price')
    for col in df.columns if hasattr(df, 'columns') and df is not None else []:
        if col not in ['price']:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass

    # 6. Parse date/time columns
    for col in df.columns if hasattr(df, 'columns') and df is not None else []:
        if 'date' in col:
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    # 7. Convert to categorical
    for col in ['brand', 'category']:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # 8. Feature Engineering
    # Extract brand from product_name if brand is unknown
    if 'product_name' in df.columns and 'brand' in df.columns:
        unknown_brands = df['brand'] == 'Unknown'
        df.loc[unknown_brands, 'brand'] = (
            df.loc[unknown_brands, 'product_name']
              .str.extract(r'(\b[A-Z][a-zA-Z]+\b)', expand=False)
              .fillna('Unknown')
        )

    # Flag: is_stainless (if 'stainless' in any text column)
    new_columns = {}
    new_columns['is_stainless'] = df.apply(lambda row: any('stainless' in str(v).lower() for v in row.values), axis=1)
    df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)

    # 9. Drop columns with >95% missing values
    to_drop = [col for col in (df.columns if hasattr(df, 'columns') and df is not None else []) if df[col].isnull().mean() > 0.95]
    df = df.drop(columns=to_drop)

    # 10. Merge company and brand into a single field
    if set(['company_name', 'brand']).issubset(df.columns):
        df['company_brand'] = df['company_name'] + " | " + df['brand']

    # 11. Clean product_name: remove special chars, lower case
    if 'product_name' in df.columns:
        df['product_name_clean'] = (
            df['product_name']
              .str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)
              .str.lower()
        )

    # 12. Normalize units in capacity column to liters (if present)
    if 'capacity' in df.columns:
        def to_liters(val):
            if pd.isnull(val):
                return np.nan
            txt = str(val).lower()
            m = re.match(r'([\d.]+)\s*([a-z]*)', txt)
            if not m:
                return np.nan
            num, unit = m.groups()
            try:
                num = float(num)
            except:
                return np.nan
            if unit.startswith('l'):   # liter
                return num
            elif unit.startswith('ml'): # milliliter
                return num / 1000
            elif unit.startswith('kl'): # kiloliter
                return num * 1000
            elif unit.startswith('g'): # gallon (approx to liter)
                return num * 3.785
            else:
                return num # assume liters if unknown
        df['capacity_liters'] = df['capacity'].apply(to_liters)

    # 13. Add a column: row number (for tracking)
    df['row_num'] = range(1, len(df) + 1)

    return df
