#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import shutil
from datetime import datetime

# Paths
RAW_DIR = "../raw_data"       # where your ingested CSVs are stored
DATA_LAKE_DIR = "data_lake/raw"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(DATA_LAKE_DIR, exist_ok=True)

def store_in_local_datalake(source: str, dataset_name: str, file_name: str):
    """Store ingested file into local data lake with partitioned structure."""
    # create timestamp partition
    timestamp = datetime.now().strftime("%Y-%m-%d")

    # target path
    target_dir = os.path.join(DATA_LAKE_DIR, source, dataset_name, timestamp)
    os.makedirs(target_dir, exist_ok=True)

    # move/copy file
    src_file = os.path.join(RAW_DIR, file_name)
    dst_file = os.path.join(target_dir, file_name)
    shutil.copy(src_file, dst_file)   # use copy, not move

    print(f"✅ Stored {file_name} into {dst_file}")

# Example usage:
store_in_local_datalake("huggingface", "churn_modelling", "churn_modelling.csv")
store_in_local_datalake("kaggle", "telco_customer_churn", "WA_Fn-UseC_-Telco-Customer-Churn.csv")

