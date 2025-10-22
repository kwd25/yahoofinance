# Yahoo Finance Data Pipeline

This project automates the collection, processing, and visualization of S&P 500 financial market data from Yahoo Finance. It is built around a Python-based workflow with Databricks integration and a Streamlit dashboard for displaying analytics.

## Overview

The goal of this project is to create an end-to-end data pipeline that collects raw financial data, preprocesses and cleans it, and then visualizes the results in an interactive dashboard. The project uses GitHub Actions for automation, Databricks for storage and transformation, and Streamlit for visualization.

## Project Structure

```
/yfinance-project
│
├── .devcontainer/          # Dev container setup for consistent local environments
├── .github/workflows/      # GitHub Actions workflows for automated ingestion
├── .streamlit/             # Streamlit configuration files
│
├── app.py                  # Streamlit dashboard app for displaying visualizations
├── ingest.py               # Script to pull raw data from Yahoo Finance into Databricks
├── preprocess.py           # Cleans and processes raw data before visualization
├── requirements.txt        # Python dependencies
└── README.md               # Project overview and instructions
```

## How It Works

1. **Data Ingestion**
   The `ingest.py` script retrieves stock data and metrics from Yahoo Finance. It runs automatically through a scheduled GitHub Action defined in `.github/workflows/daily_ingest.yml`. The script uploads data to Databricks tables for storage and further processing.

2. **Preprocessing and Transformation**
   The `preprocess.py` script performs data cleaning, adds derived features, and ensures the dataset is formatted for analysis. It prepares the data used by the dashboard.

3. **Visualization**
   The `app.py` file runs a Streamlit dashboard that visualizes financial indicators such as moving averages, momentum, and volatility. The dashboard connects to Databricks or local data outputs and allows interactive filtering and exploration.

4. **Automation**
   The GitHub workflow (`daily_ingest.yml`) runs the ingestion script on a daily schedule, ensuring that the data is continuously updated without manual intervention.

## Requirements

* Python 3.10+
* pandas
* yfinance
* databricks-sql-connector
* pyarrow
* streamlit

Install dependencies using:

```
pip install -r requirements.txt
```

## Usage

1. Clone the repository:

   ```
   git clone https://github.com/kwd25/yfinance-project.git
   cd yfinance-project
   ```

2. Run the ingestion script manually:

   ```
   python ingest.py
   ```

3. Optionally run preprocessing:

   ```
   python preprocess.py
   ```

4. Launch the Streamlit dashboard:

   ```
   streamlit run app.py
   ```

## Notes

* Make sure your Databricks credentials are configured before running ingestion.
* Logs and automated ingestion runs can be viewed in the GitHub Actions tab.
* Streamlit configuration is handled in `.streamlit/config.toml`.

