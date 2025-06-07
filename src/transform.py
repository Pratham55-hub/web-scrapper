import duckdb
import pandas as pd
import numpy as np
import re

# ----- 1. LOAD THE DATA -----
csv_path = 'scraped_data.csv'  # <-- update path as needed

df = duckdb.query(f"SELECT * FROM '{csv_path}'").to_df()

print("\n=== Original Columns and Shape ===")
print(df.columns)
print(df.shape)

# ----- 2. DATA CLEANING -----
# Remove duplicates (prefer Product Link, else Product Name)
if 'Product Link' in df.columns:
    df = df.drop_duplicates(subset=['Product Link'])
elif 'Product Name' in df.columns:
    df = df.drop_duplicates(subset=['Product Name'])
else:
    df = df.drop_duplicates()

# Standardize column names
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

# Trim whitespace from all string columns
str_cols = df.select_dtypes(include='object').columns
df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

# Fill missing company_name/brand with 'Unknown'
for col in ['company_name', 'brand']:
    if col in df.columns:
        df[col] = df[col].fillna('Unknown')

# ----- 3. DATA TYPE CONVERSION -----
# Convert likely numerics (except price)
for col in df.columns:
    if col not in ['price']:  # explicitly skip price
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            pass

# Parse date/time columns
for col in df.columns:
    if 'date' in col or 'time' in col:
        try:
            df[col] = pd.to_datetime(df[col])
        except Exception:
            pass

# Convert categorical columns
for col in ['brand', 'category']:
    if col in df.columns:
        df[col] = df[col].astype('category')

# ----- 4. FEATURE ENGINEERING -----
# Example: Extract brand from product_name if brand is unknown
if 'product_name' in df.columns and 'brand' in df.columns:
    unknown_brands = df['brand'] == 'Unknown'
    df.loc[unknown_brands, 'brand'] = (
        df.loc[unknown_brands, 'product_name']
        .str.extract(r'(\b[A-Z][a-zA-Z]+\b)', expand=False)
        .fillna('Unknown')
    )

# Create a flag: is_stainless (if 'stainless' in any text column)
df['is_stainless'] = df.apply(lambda row: any('stainless' in str(v).lower() for v in row.values), axis=1)

# ----- 5. FILTERING AND AGGREGATION -----
# Example: Print count of products by brand (not analysis, just a print)
if 'brand' in df.columns:
    print("\n=== Product count by brand ===")
    print(df['brand'].value_counts())

# ----- 6. COLUMN/ROW OPERATIONS -----
# Drop columns with >95% missing values
to_drop = [col for col in df.columns if df[col].isnull().mean() > 0.95]
df = df.drop(columns=to_drop)

# Merge company and brand into a single field
if set(['company_name','brand']).issubset(df.columns):
    df['company_brand'] = df['company_name'] + " | " + df['brand']

# ----- 7. TEXT PROCESSING -----
# Clean product_name: remove special chars, lower case
if 'product_name' in df.columns:
    df['product_name_clean'] = (
        df['product_name']
        .str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)
        .str.lower()
    )

# Normalize units in capacity column to liters (if present)
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

# ----- 8. ENRICHMENT -----
# Add a column: row number (for tracking)
df['row_num'] = range(1, len(df) + 1)

# === Preview and Save ===
print("\n=== Preview of transformed data ===")
print(df.head())

df.to_parquet("scraped_data_transformed.parquet", index=False)
print("\nTransformed data saved to scraped_data_transformed.parquet")
