from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize BigQuery client
client = bigquery.Client(project=os.getenv("GCP_PROJECT_ID"))

# Function to load data to BigQuery
def load_data_to_bigquery(file_path, table_name):
    # Load CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Convert DataFrame to BigQuery schema
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
            bigquery.SchemaField("street", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("postal_code", "STRING")
        ] if table_name == "users" else
        [
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("brand", "STRING"),
            bigquery.SchemaField("price", "FLOAT")
        ] if table_name == "products" else
        [
            bigquery.SchemaField("cart_id", "INTEGER"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("total_cart_value", "FLOAT")
        ]
    )
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = False  # We've manually defined the schema
    job_config.skip_leading_rows = 1  # Skip header row
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrites table if exists

    # Construct the full table ID
    table_id = f"{os.getenv('BIGQUERY_DATASET')}.{table_name}"
    
    # Load data to BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete

    print(f"Loaded {job.output_rows} rows into {table_id}")

# Main function to execute loading process
def main():
    load_data_to_bigquery('data/cleaned_users.csv', 'users')
    load_data_to_bigquery('data/cleaned_products.csv', 'products')
    load_data_to_bigquery('data/cleaned_carts.csv', 'carts')

    print("Data loading into BigQuery completed successfully.")

if __name__ == "__main__":
    main()