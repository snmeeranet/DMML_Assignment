#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
from datetime import datetime

RAW_DIR = "../raw_data"
REPORTS_DIR = "reports"

# Ensure directory exists
os.makedirs(REPORTS_DIR, exist_ok=True)

def validate_dataset(file_path):
    df = pd.read_csv(file_path)

    issues = []
    resolutions = []

    # 1. Missing values
    missing = df.isnull().sum()
    for col, count in missing.items():
        if count > 0:
            issues.append(f"{count} missing values in '{col}'")
            if df[col].dtype in ["int64", "float64"]:
                df[col].fillna(df[col].mean(), inplace=True)
                resolutions.append(f"Filled missing '{col}' with column mean")
            else:
                df[col].fillna("UNKNOWN", inplace=True)
                resolutions.append(f"Filled missing '{col}' with 'UNKNOWN'")

    # 2. Duplicates
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        issues.append(f"{dup_count} duplicate rows found")
        df.drop_duplicates(inplace=True)
        resolutions.append("Removed duplicate rows")

    # 3. Numeric out-of-range (example check: Age)
    if "Age" in df.columns:
        invalid_age = df[(df["Age"] < 0) | (df["Age"] > 120)]
        if not invalid_age.empty:
            issues.append(f"{len(invalid_age)} invalid Age values")
            df.loc[(df["Age"] < 0) | (df["Age"] > 120), "Age"] = None
            resolutions.append("Replaced invalid Age values with NULL")

    # 4. Data type issues
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col].astype(float)
                issues.append(f"Column '{col}' may be numeric stored as string")
                resolutions.append(f"Suggested: convert '{col}' to numeric")
            except:
                pass

    # Save cleaned dataset
    cleaned_file = file_path.replace(".csv", "_cleaned.csv")
    df.to_csv(cleaned_file, index=False)

    # Return report row
    return {
        "file": os.path.basename(file_path),
        "rows": len(df),
        "columns": len(df.columns),
        "issues": "; ".join(issues) if issues else "No issues",
        "resolutions": "; ".join(resolutions) if resolutions else "No resolutions"
    }

def main():
    all_reports = []
    for file in os.listdir(RAW_DIR):
        if file.endswith(".csv"):
            path = os.path.join(RAW_DIR, file)
            print(f"Validating {file}...")
            report = validate_dataset(path)
            all_reports.append(report)

    # Save report
    report_df = pd.DataFrame(all_reports)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(REPORTS_DIR, f"data_quality_report_{timestamp}.csv")
    report_df.to_csv(report_file, index=False)

    print(f"✅ Data quality report saved at: {report_file}")

if __name__ == "__main__":
    main()

