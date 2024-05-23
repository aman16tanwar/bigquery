from google.cloud import bigquery
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the BigQuery client
client = bigquery.Client()

# Define your dataset
dataset_id = 'sun-peaks-resort.client_ua'

# Get the list of tables in the dataset
tables = list(client.list_tables(dataset_id))
logging.info(f"Found {len(tables)} tables in dataset {dataset_id}")

# Prepare the result list
results = []

for table in tables:
    table_id = f"{dataset_id}.{table.table_id}"
    logging.info(f"Processing table {table_id}")
    
    # Query to find the earliest and latest date in the 'date' column
    query = f"""
    SELECT
      MIN(date) AS earliest_date,
      MAX(date) AS latest_date
    FROM `{table_id}`
    WHERE
      date IS NOT NULL
    """
    
    try:
        # Run the query
        query_job = client.query(query)
        result = query_job.result().to_dataframe()
        
        if not result.empty:
            results.append((table.table_id, result.iloc[0]))
        else:
            logging.warning(f"No date column found or no data in table {table.table_id}")
    except Exception as e:
        logging.error(f"Error processing table {table.table_id}: {e}")

# Convert results to a DataFrame
results_df = pd.DataFrame(results, columns=['table_name', 'date_range'])
if not results_df.empty:
    results_df['earliest_date'] = results_df['date_range'].apply(lambda x: x['earliest_date'])
    results_df['latest_date'] = results_df['date_range'].apply(lambda x: x['latest_date'])
    results_df.drop(columns=['date_range'], inplace=True)

    # Display the results
    print("Table Date Ranges:")
    print(results_df)
    
    # Optionally, save to a CSV file
    results_df.to_csv("table_date_ranges.csv", index=False)
    logging.info("Results saved to table_date_ranges.csv")
else:
    logging.info("No tables with a date column were found or processed successfully.")
