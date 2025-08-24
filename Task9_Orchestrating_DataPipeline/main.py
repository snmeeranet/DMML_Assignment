# orchestrator.py
from prefect import flow, task
import subprocess

# ---- Wrap each main.py as a task ----
@task
def run_main(path):
    subprocess.run(["python3", f"{path}/main.py"], check=True)

# ---- Define the flow ----
@flow(name="Churn ML Pipeline")
def churn_pipeline():
    run_main("Task1_DataIngestion/DataIngestionImplementation.ipynb")   # ingestion
    # run_main("Raw_data_storage")  # Run Data Storage
    # run_main("Data_validation")  # validation
    # run_main("Data_prepration")  # Data Preparation
    # run_main("Data_Transformation_and_storage")  # Data Transformation and storage
    # run_main("Feature_store")  # feature store
    # run_main("Model_building")  # Model Building
    # run_main("Data_verisoning")  # Model Building

# ---- Run ----
if __name__ == "__main__":
    churn_pipeline()
