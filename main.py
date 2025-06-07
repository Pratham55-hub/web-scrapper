import yaml
import argparse
from etl.extract import extract
from etl.transform import transform
from etl.load import load

def parse_args():
    parser = argparse.ArgumentParser(description="Configurable ETL for B2B web-scraping")
    parser.add_argument("--config", type=str, default="config/scrape_config.yaml", help="Path to config yaml")
    parser.add_argument("--site", type=str, default="tradeindia", help="Site key from config")
    parser.add_argument("--max_pages", type=int, default=3, help="Pages per category")
    parser.add_argument("--output_format", type=str, default="parquet", choices=["parquet", "csv"])
    return parser.parse_args()

def main():
    args = parse_args()
    print("=== Initiating ETL ===")

    with open(args.config, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    site_config = config[args.site]
    site_config['max_pages'] = args.max_pages
    site_config['output_format'] = args.output_format

    print("=== EXTRACT ===")
    df_raw = extract(site_config)
    print(f"Extracted {len(df_raw)} rows.")

    print("=== TRANSFORM ===")
    df_clean = transform(df_raw, site_config)
    print(f"Transformed shape: {df_clean.shape}")

    print("=== LOAD ===")
    load(df_clean, site_config)
    print("ETL complete.")

if __name__ == "__main__":
    main()
