import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery


#Centralizing all config here. For full mature project, I'll place this in YAML
PROJECT_ID = "fintech-credit-ledger"
DATASET_ID = "bronze"
TABLE_ID = "raw_transactions"

#Logging setup
logging.basicConfig (
    level = logging.INFO,
    format= "%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

def create_raw_ledger(rows=1000)-> pd.DataFrame:
    
    """
    Generates intentionally messy transaction ledger.
    Simulates real-world data quality issues: duplicates, nulls, invalid values.
    Args: rows is number of base rows before duplicates are added.
    Returns:A pandas DataFrame representing the raw ledger.
    """

    log.info("Starting FinTech data generation...")
    np.random.seed(42)

    data = {
        #Generating unique IDs for transactions
        'txn_id': [f"TXN_{1000+i}" for i in range(rows)],

        #Simulating 100 unique customers
        'user_id': [f"USER_{np.random.randint(1, 100)}" for _ in range(rows)],

        #Generating amounts, creating refund errors and some very high outliers
        'amount': np.random.uniform(-100, 5000, rows),

        #Transaction currencies for the dataset
        'currency': np.random.choice(['USD', 'EUR', 'GBP', 'INVALID'], rows, p=[0.7, 0.15, 0.1, 0.05]),

        #Generating timestamps for the past 7 days ~
        'timestamp': [datetime.now() - timedelta(minutes=np.random.randint(0, 1000)) for _ in range(rows)],

        'status': np.random.choice(['success', 'pending', 'failed'], rows)
    }

    df = pd.DataFrame(data) #Converting the dictionary into a "table" format (Data frame)
    df['amount'] = df['amount'].round(2)
    
    #Simulation of real world datasets with duplicates
    df = pd.concat([df, df.head(10)], ignore_index = True)
    log.info(f"Added 10 duplicate rows. Total rows: {len(df)}")

    #Simulation of real world datasets with 2) NULLS, takes 5% (0.05) of 'amount' column \ 
    # deleting them (NaN), handling to be done with SQL
    df.loc[df.sample(frac=0.05).index, 'amount'] = np.nan
    return df

def save_to_csv(df: pd.DataFrame) ->str:
    #Saving the DataFrame to a CSV file alongside this script
    folder = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(folder, "raw_transactions.csv")

    #Output for the table saving it as actual file in absolute path
    df.to_csv(path, index=False)
    log.info(f"CSV saved to: {path} ({len(df)} rows)")
    return path

def upload_to_bigquery(df: pd.DataFrame) -> None:
    #Uploading the DataFrame to BigQuery  as the Bronze layer
    client = bigquery.Client(project = PROJECT_ID)

    #This block ensures the dataset exists and creates it if its not available
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    log.info(f"Dataset '{DATASET_ID}' is ready.")

        #Explicitly defining the BigQuery schema for consistency and integrity
    schema = [
        bigquery.SchemaField("txn_id",    "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("user_id",   "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("amount",    "FLOAT64",   mode="NULLABLE"),
        bigquery.SchemaField("currency",  "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status",    "STRING",    mode="NULLABLE"),
    ]

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    #Write truncate will give us the same data on pipeline without duplicating rows
    job_config = bigquery.LoadJobConfig(
        schema = schema,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    log.info(f"Uploading to BigQuery table: {table_ref}")
    job = client.load_table_from_dataframe(df, table_ref, job_config = job_config)
    job.result()  #Waits for the job to complete and raises on failure

    table = client.get_table(table_ref)
    log.info(f"SUCCESS: {table.num_rows} rows loaded into {table_ref}")

if __name__ == "__main__":
    df = create_raw_ledger(rows=1000)
    save_to_csv(df)
    upload_to_bigquery(df)