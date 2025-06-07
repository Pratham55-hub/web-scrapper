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

> **Prerequisite:**  
> Python 3.13 or higher is required.  
> Download from [python.org](https://www.python.org/downloads/).
> Jupyter Notebook or JupyterLab

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

## 📝 Next Steps: Data Analysis in Jupyter

1. **Install Jupyter Notebook**

    ```bash
    pip install notebook
    ```

2. **Install Required Python Packages**

    Make sure you have the following packages:

    ```bash
    pip install pandas numpy matplotlib seaborn duckdb
    ```

    If you plan to use additional packages (e.g., `wordcloud`):

    ```bash
    pip install wordcloud
    ```

3. **Prepare Your Data**

    Place your cleaned data file (e.g., `scraped_data_transformed.parquet` ) in the same directory as your notebook, or update the notebook paths accordingly.

4. **Launch Jupyter Notebook**

    In your project folder, run:

    ```bash
    jupyter notebook
    ```

    Or, for JupyterLab:

    ```bash
    jupyter lab
    ```

    This will open Jupyter in your browser.

5. **Open and Run the Notebook**

    - Click on `analysis.ipynb` in the Jupyter dashboard.
    - Run each cell sequentially (select a cell and press `Shift+Enter`), or use `Cell > Run All` for a complete run.

6. **Export Results (Optional)**

    Download the notebook as HTML or PDF from the menu:  
    `File > Download as > HTML (.html)` or `PDF (.pdf)`  
    (For PDF export, you may need to install TeX. See Jupyter documentation for details.)

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