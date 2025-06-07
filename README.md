# web-scrapper

Web-Scrapper ETL Pipeline


A modular, configurable ETL pipeline for scraping and analyzing product data from B2B marketplaces such as IndiaMART, Alibaba, and TradeIndia.
The project uses Playwright and Pandas for robust data extraction, cleaning, and output.

🚀 Features

Configurable YAML: Easily add/modify scraping targets and selectors.

Headless Browser Automation: Resilient scraping with Playwright.

Persistent Deduplication: Never scrape the same product twice.

Clean ETL Modularity: Separate extraction, transformation, and loading logic.

Multiple Output Formats: Parquet (default) and CSV supported.

Ready for EDA: Outputs clean data for analytics and visualization.

📁 Project Structure

```
web-scrapper/
├── data/                # Output data (parquet/csv)
├── etl/
│   ├── extract.py       # Extraction logic (Playwright)
│   ├── transform.py     # Data cleaning and transformation
│   └── load.py          # Output to file/DB
├── config/
    ├──scrape_config.yaml   # Scraping targets and selectors
├── requirements.txt     # Python dependencies
├── README.md            # Project documentation
└── notebooks/           # EDA and analysis notebooks

```

⚡️ Quickstart

Clone the repository:
```bash
git clone https://github.com/yourusername/web-scrapper.git
cd web-scrapper
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Install Playwright browsers:

```bash
playwright install
```

Run the ETL pipeline:

```bash
python main.py --config config/scrape_config.yaml --site tradeindia --max_pages 3 --output_format parquet
```


Output is saved in the data/ directory.

⚙️ Configuration

The YAML config drives all site-specific scraping settings:
```yaml
tradeindia:
    base_url: https://example.com
    selectors:
        product_card: ".product-card"
        product_name: ".product-title"
        company_name: ".company"
        product_link: "a.link"
        spec_table: ".spec-table"
        popup_close: ".close-btn"
        next_button: ".next-page"
    urls:
        categories:
            - name: Electronics
                endpoint: /electronics
            - name: Machinery
                endpoint: /machinery
    already_scraped_file: data/already_scraped.txt
```
Tip: Add as many sites or categories as you want—just update the YAML.


🛠️ Extending

Add a new marketplace: Just add a new block to scrape_config.yaml.

New output types: Extend etl/load.py for DB, Excel, or cloud storage.

Change pipeline logic: Tweak modular ETL scripts; each stage is independent.

📊 Data Analysis

After scraping, use Jupyter, Pandas, or your favorite analytics tools on data/*.parquet or data/*.csv.

Sample EDA notebooks are recommended for deeper exploration.

🐞 Troubleshooting

Selectors outdated: Update your YAML if site structure changes.

Blocked scraping: Try longer delays/randomness in extract.py.

Type errors: Ensure DataFrames are always passed and validated between ETL stages.

🤝 Contributing

Pull requests are welcome! Please open an issue first to discuss major changes or ideas.

📄 License

This project is licensed under the MIT License.

✨ Author
Pratham