from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_bigquery_connectivity():
    try:
        # Initialize the BigQuery client
        client = bigquery.Client()

        # Get project and dataset details from environment variables
        project_id = os.getenv("GCP_PROJECT_ID")
        dataset_name = os.getenv("BIGQUERY_DATASET")

        # Construct the dataset reference
        dataset_id = f"{project_id}.{dataset_name}"

        # Fetch the dataset
        dataset = client.get_dataset(dataset_id)

        # If successful, print dataset details
        print(f"Connected to BigQuery dataset: {dataset_id}")
        print(f"Dataset Description: {dataset.description}")
        print(f"Dataset Location: {dataset.location}")
    except Exception as e:
        print(f"Failed to connect to BigQuery: {e}")

# Ensure the script runs only when executed directly
if __name__ == "__main__":
    test_bigquery_connectivity()
