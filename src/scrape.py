import time
import random
import yaml
import pandas as pd
import os
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

def load_config(config_file, site):
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    if site not in config:
        raise ValueError(f"Site '{site}' not found in {config_file}")
    site_conf = config[site]
    selectors = site_conf['selectors']
    base_url = site_conf['base_url']
    categories = site_conf['urls']['categories']
    return selectors, base_url, categories

def extract_spec_table(page, spec_table_selector):
    specs = {}
    table = page.query_selector(spec_table_selector)
    if table:
        for row in table.query_selector_all('tr'):
            cells = row.query_selector_all('td')
            if len(cells) == 2:
                key = cells[0].inner_text().strip()
                value = cells[1].inner_text().strip()
                specs[key] = value
    return specs

def extract_card_data(card_handle, selectors):
    data = {}
    for field, selector in [('Product Name', selectors['product_name']),
                            ('Company Name', selectors['company_name']),
                            ('Product Link', selectors['product_link'])]:
        el = card_handle.query_selector(selector)
        if el:
            data[field] = el.inner_text().strip() if field != 'Product Link' else el.get_attribute('href')
    return data

def scrape_page(page, selectors, category_name, already_scraped):
    product_cards = page.query_selector_all(selectors['product_card'])
    print(f"    Found {len(product_cards)} product cards using selector '{selectors['product_card']}'")

    data_rows = []
    for card_handle in product_cards:
        card_data = extract_card_data(card_handle, selectors)
        product_url = card_data.get('Product Link')

        # --- De-duplication: skip if already scraped
        if not product_url or product_url in already_scraped:
            continue
        already_scraped.add(product_url)  # Mark as scraped immediately

        card_data['Category'] = category_name
        if product_url:
            detail_page = page.context.new_page()
            try:
                detail_page.goto(product_url, timeout=30000)
                detail_page.wait_for_load_state("load")
                time.sleep(random.uniform(3, 8))
                specs = extract_spec_table(detail_page, selectors['spec_table'])
                card_data.update(specs)
            except Exception as e:
                print(f"Failed to scrape detail for {product_url}: {e}")
            finally:
                detail_page.close()
        else:
            print(f"No product link found in card: {card_data}")
        data_rows.append(card_data)
        time.sleep(random.uniform(0.5, 1.5))
    return data_rows

def run(site, config_file, max_pages):
    selectors, base_url, categories = load_config(config_file, site)
    all_data = []
    all_keys = set()
    parquet_file = f"{site}_scraped_data.parquet"

    # --- Persistent de-duplication: Load already-scraped links ---
    already_scraped = set()
    if os.path.exists(parquet_file):
        df_prev = pd.read_parquet(parquet_file)
        if "Product Link" in df_prev.columns:
            already_scraped = set(df_prev["Product Link"].dropna())
        print(f"Loaded {len(already_scraped)} already-scraped products.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for cat in categories:
            name = cat['name']
            endpoint = cat['endpoint']
            url = urljoin(base_url, endpoint)
            print(f"Scraping category: {name}")
            page.goto(url)
            page.wait_for_load_state("load")
            time.sleep(5)

            for page_num in range(1, max_pages + 1):
                print(f"  Scraping page {page_num}")

                # Close popup if exists (configurable)
                popup_close_btn = page.query_selector(selectors['popup_close'])
                if popup_close_btn and popup_close_btn.is_visible():
                    print("  Closing popup overlay...")
                    popup_close_btn.click()
                    time.sleep(1)

                data_rows = scrape_page(page, selectors, name, already_scraped)
                all_data.extend(data_rows)
                for row in data_rows:
                    all_keys.update(row.keys())

                if page_num < max_pages:
                    popup_close_btn = page.query_selector(selectors['popup_close'])
                    if popup_close_btn and popup_close_btn.is_visible():
                        print("  Closing popup overlay (post-scrape)...")
                        popup_close_btn.click()
                        time.sleep(1)

                    next_button = page.query_selector(selectors['next_button'])
                    if next_button and next_button.is_enabled() and next_button.is_visible():
                        delay = random.uniform(2, 5)
                        print(f"  Clicking Next... (waiting {delay:.1f} seconds)")
                        next_button.click()
                        page.wait_for_load_state("load")
                        time.sleep(delay)
                    else:
                        print("  Next button is not clickable or not found. Stopping.")
                        break

        browser.close()

    # --- Merge with previous data and deduplicate ---
    df_new = pd.DataFrame(all_data)
    if os.path.exists(parquet_file):
        df_prev = pd.read_parquet(parquet_file)
        df = pd.concat([df_prev, df_new], ignore_index=True)
    else:
        df = df_new

    if "Product Link" in df.columns:
        df = df.drop_duplicates(subset=["Product Link"])

    df.to_parquet(parquet_file, index=False)
    print(f"Data saved to {parquet_file} (total {len(df)} unique products)")

if __name__ == "__main__":
    run(site="tradeindia", config_file="scrape_config.yaml", max_pages=6)
