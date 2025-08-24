#!/usr/bin/env python
# coding: utf-8

# In[5]:


get_ipython().system('pip install matplotlib')
get_ipython().system('pip install seaborn')
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
import os
import matplotlib.pyplot as plt
import os
from datetime import datetime


sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (10,6)

PROCESSED_DIR =  "processed_data"
os.makedirs(PROCESSED_DIR, exist_ok=True)


# In[6]:


def clean_dataset(df: pd.DataFrame, dataset_name: str):
    print(f"\n--- Cleaning {dataset_name} ---")

    # 1. Drop duplicates
    before = df.shape[0]
    df = df.drop_duplicates()
    after = df.shape[0]
    print(f"Removed {before - after} duplicate rows")

    # 2. Handle missing values
    missing_summary = df.isnull().sum()
    print("Missing values:\n", missing_summary[missing_summary > 0])

    # Strategy: fill numeric with median, categorical with mode
    for col in df.columns:
        if df[col].dtype in ["int64", "float64"]:
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna(df[col].mode()[0])

    # 3. Validate datatypes (convert object numerics)
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_numeric(df[col])
                print(f"Converted {col} to numeric")
            except:
                pass  # keep categorical

    return df

def preprocess_dataset(df: pd.DataFrame, dataset_name: str):
    print(f"\n--- Preprocessing {dataset_name} ---")

    # Separate features & target (assume 'Churn' or 'Exited' is target)
    target_col = None
    for cand in ["Churn", "Exited", "churn", "exited"]:
        if cand in df.columns:
            target_col = cand
            break

    if not target_col:
        raise ValueError(f"No churn target found in {dataset_name}")

    X = df.drop(columns=[target_col])
    y = df[target_col]

    # Encode categorical vars
    cat_cols = X.select_dtypes(include="object").columns
    for col in cat_cols:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col])
        print(f"Encoded {col}")

    # Scale numeric features
    num_cols = X.select_dtypes(include=["int64", "float64"]).columns
    scaler = StandardScaler()
    X[num_cols] = scaler.fit_transform(X[num_cols])

    # Merge back target
    df_prepared = pd.concat([X, y], axis=1)
    return df_prepared

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# --- Generate basic EDA ---
def generate_eda(df, file_prefix):
    os.makedirs(os.path.join(PROCESSED_DIR, "visualizations"), exist_ok=True)
    
    # Histograms
    df.hist(bins=15, figsize=(12, 8))
    plt.tight_layout()
    hist_path = os.path.join(PROCESSED_DIR, "visualizations", f"{file_prefix}_hist.png")
    plt.savefig(hist_path)
    plt.close()
    
    # Boxplots
    plt.figure(figsize=(12,6))
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    sns.boxplot(data=df[numeric_cols])
    box_path = os.path.join(PROCESSED_DIR, "visualizations", f"{file_prefix}_boxplot.png")
    plt.savefig(box_path)
    plt.close()
    
    print(f"EDA visualizations saved → {hist_path}, {box_path}")


# In[7]:


kaggle_path = "../raw_data/churn_modelling_cleaned.csv"
hf_path = "../raw_data/WA_Fn-UseC_-Telco-Customer-Churn_cleaned.csv"

df_kaggle = pd.read_csv(kaggle_path)
df_hf = pd.read_csv(hf_path)

print("Kaggle Dataset Shape:", df_kaggle.shape)
print("HuggingFace Dataset Shape:", df_hf.shape)


# In[8]:


df_kaggle_clean = clean_dataset(df_kaggle, "../raw_data/churn_modelling_cleaned.csv")
df_hf_clean = clean_dataset(df_hf, "../raw_data/WA_Fn-UseC_-Telco-Customer-Churn_cleaned.csv")

df_kaggle_prep = preprocess_dataset(df_kaggle_clean, "Kaggle")
df_hf_prep = preprocess_dataset(df_hf_clean, "HuggingFace")

df_kaggle_prep.to_csv("processed_data/clean_kaggle.csv", index=False)
df_hf_prep.to_csv("processed_data/clean_hf.csv", index=False)

df_kaggle = pd.read_csv(r"processed_data/clean_kaggle.csv")
generate_eda(df_kaggle, f"csv_{timestamp}")

df_hf = pd.read_csv(r"processed_data/clean_hf.csv")
generate_eda(df_hf, f"csv_{timestamp}")
