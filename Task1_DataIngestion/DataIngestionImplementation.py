#!/usr/bin/env python
# coding: utf-8

# In[1]:


# src/ingestion/ingest_excels.py
import os
import pandas as pd
from datetime import datetime
import logging
import requests

# --- Directories ---
RAW_DATA_DIR = "raw_data"
LOG_DIR = "logs"
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# --- Logging setup ---
LOG_FILE = os.path.join(LOG_DIR, "ingestion.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_URL = "https://jsonplaceholder.typicode.com/users"

DATA_SOURCES = {
    "churn_modelling": "../raw_data/churn_modelling.csv",
    "telco_churn": "../raw_data/WA_Fn-UseC_-Telco-Customer-Churn.csv"
}

# --- Column name cleaning ---
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'[\s\-]+', '_', regex=True)
        .str.replace(r'[^a-z0-9_]', '', regex=True)
        .str.replace(r'_+', '_', regex=True)
        .str.rstrip('_')
    )
    rename_map = {
        "customerid": "customer_id",
        "customer_id_": "customer_id",
        "customerid_": "customer_id"
    }
    df = df.rename(columns={col: rename_map[col] for col in df.columns if col in rename_map})
    return df


# --- CSV Ingestion ---
def ingest_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        df = clean_column_names(df)
        output_file = os.path.join(
            RAW_DATA_DIR, f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        df.to_csv(output_file, index=False)
        logging.info(f"CSV ingestion successful. File saved: {output_file}")
        print(f"[CSV] Ingestion successful → {output_file}")
        return output_file
    except Exception as e:
        logging.error(f"CSV ingestion failed: {e}")
        print(f"[CSV] Ingestion failed: {e}")
        return None


# --- API Ingestion ---
def ingest_api(api_url):
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        df = clean_column_names(df)
        output_file = os.path.join(
            RAW_DATA_DIR, f"api_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        df.to_csv(output_file, index=False)
        logging.info(f"API ingestion successful. File saved: {output_file}")
        print(f"[API] Ingestion successful → {output_file}")
        return output_file
    except Exception as e:
        logging.error(f"API ingestion failed: {e}")
        print(f"[API] Ingestion failed: {e}")
        return None


# --- Excel/CSV Ingestion ---
def ingest_excel(source_name, filepath):
    """Ingest a single Excel/CSV file into raw storage with timestamped filename."""
    try:
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)

        df = clean_column_names(df)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join(RAW_DATA_DIR, source_name)
        os.makedirs(out_dir, exist_ok=True)

        out_path = os.path.join(out_dir, f"{source_name}_{timestamp}.csv")
        df.to_csv(out_path, index=False)

        logging.info(f"SUCCESS - Ingested {source_name} into {out_path} with {len(df)} rows.")
        print(f"[OK] {source_name} → {out_path} ({len(df)} rows)")
        return out_path
    except Exception as e:
        logging.error(f"FAILED - {source_name} ingestion failed: {str(e)}")
        print(f"[ERR] {source_name} ingestion failed: {str(e)}")
        return None


# --- Run all ingestions ---
def run_ingestion():
    """Ingest all configured data sources."""
    for source, path in DATA_SOURCES.items():
        ingest_excel(source, path)

    # Optionally run API ingestion too
    ingest_api(API_URL)


if __name__ == "__main__":
    run_ingestion()

