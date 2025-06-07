# etl/extract.py

import time
import random
import pandas as pd
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import os

def extract(config: dict) -> pd.DataFrame:
    """
    Extracts product data from a B2B site as defined in config.
    Uses a persistent file to avoid scraping already scraped product links.
    Returns a pandas DataFrame with raw, wide data.
    """
    selectors = config['selectors']
    base_url = config['base_url']
    categories = config['urls']['categories']
    print(categories)
    max_pages = config.get('max_pages', 3)
    already_scraped_file = config.get('already_scraped_file', 'data/already_scraped.txt')

    # --- Load already scraped links ---
    if os.path.exists(already_scraped_file):
        with open(already_scraped_file, 'r', encoding='utf-8') as f:
            already_scraped = set(line.strip() for line in f if line.strip())
        print(f"Loaded {len(already_scraped)} already-scraped product links from {already_scraped_file}")
    else:
        already_scraped = set()
        print(f"No existing already-scraped file at {already_scraped_file}, starting fresh.")

    def append_to_already_scraped(link):
        with open(already_scraped_file, 'a', encoding='utf-8') as f:
            f.write(link + "\n")

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
        for field, selector in [
            ('Product Name', selectors['product_name']),
            ('Company Name', selectors['company_name']),
            ('Product Link', selectors['product_link'])
        ]:
            el = card_handle.query_selector(selector)
            if el:
                data[field] = el.inner_text().strip() if field != 'Product Link' else el.get_attribute('href')
        return data

    def scrape_page(page, selectors, category_name, already_scraped):
        product_cards = page.query_selector_all(selectors['product_card'])
        data_rows = []
        for card_handle in product_cards:
            card_data = extract_card_data(card_handle, selectors)
            product_url = card_data.get('Product Link')

            # De-duplication: skip if already scraped
            if not product_url or product_url in already_scraped:
                continue
            already_scraped.add(product_url)
            append_to_already_scraped(product_url)  # Immediately persist

            card_data['Category'] = category_name
            if product_url:
                detail_page = page.context.new_page()
                try:
                    detail_page.goto(product_url, timeout=30000)
                    detail_page.wait_for_load_state("load")
                    time.sleep(random.uniform(3, 7))
                    specs = extract_spec_table(detail_page, selectors['spec_table'])
                    card_data.update(specs)
                except Exception as e:
                    print(f"Failed to scrape detail for {product_url}: {e}")
                finally:
                    detail_page.close()
            data_rows.append(card_data)
            time.sleep(random.uniform(0.5, 1.5))
        return data_rows

    all_data = []
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

                # Optional: handle popup overlays
                popup_close_btn = page.query_selector(selectors.get('popup_close'))
                if popup_close_btn and popup_close_btn.is_visible():
                    print("  Closing popup overlay...")
                    popup_close_btn.click()
                    time.sleep(1)

                data_rows = scrape_page(page, selectors, name, already_scraped)
                all_data.extend(data_rows)

                # Go to next page if possible
                if page_num < max_pages:
                    popup_close_btn = page.query_selector(selectors.get('popup_close'))
                    if popup_close_btn and popup_close_btn.is_visible():
                        popup_close_btn.click()
                        time.sleep(1)

                    next_button = page.query_selector(selectors.get('next_button'))
                    if next_button and next_button.is_enabled() and next_button.is_visible():
                        delay = random.uniform(2, 5)
                        print(f"  Clicking Next... (waiting {delay:.1f} seconds)")
                        next_button.click()
                        page.wait_for_load_state("load")
                        time.sleep(delay)
                    else:
                        print("  Next button not found/clickable. Stopping.")
                        break

        browser.close()

    df = pd.DataFrame(all_data)
    # Drop duplicate product links, if present
    if "Product Link" in df.columns:
        df = df.drop_duplicates(subset=["Product Link"])
    return df
