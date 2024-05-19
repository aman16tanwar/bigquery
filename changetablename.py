from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def rename_tables_in_dataset(project_id, dataset_id, columns_to_remove=None):
    client = bigquery.Client(project=project_id)
    columns_to_remove = columns_to_remove or []

    try:
        dataset_ref = client.dataset(dataset_id)
        tables = list(client.list_tables(dataset_ref))
        print(f"Found tables in dataset {dataset_id}: {[table.table_id for table in tables]}")

        for table in tables:
            table_id = table.table_id
            table_ref = dataset_ref.table(table_id)
            table_obj = client.get_table(table_ref)
            all_columns = [schema_field.name for schema_field in table_obj.schema]

            # Keep only columns that are not in columns_to_remove
            columns_to_keep = [col for col in all_columns if col not in columns_to_remove]

            # Determine the new table name
            if not (table_id.startswith('aspenware') or table_id.startswith('chronogolf')):
                new_table_id = f'primary_{table_id}'
            else:
                new_table_id = table_id

            # Construct the SQL query
            query = f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{new_table_id}` AS
            SELECT 
              {', '.join(columns_to_keep)}
            FROM 
              `{project_id}.{dataset_id}.{table_id}`;
            """

            # Run the query
            job = client.query(query)
            job.result()  # Wait for the job to complete

            # Delete the original table if a new table was created
            if new_table_id != table_id:
                client.delete_table(table_ref)
                print(f"Renamed table {table_id} to {new_table_id} in dataset {dataset_id} without columns {', '.join(columns_to_remove)}")
            else:
                print(f"Table {table_id} in dataset {dataset_id} did not need renaming")
    
    except NotFound as e:
        print(f"Error: {e}. Dataset {dataset_id} not found in project {project_id}")

# Parameters
project_id = 'add_GCP_project_id'   # The GCP project id you are renaming tables within 
dataset_id = 'client_ua'  # The dataset you are renaming tables within
columns_to_remove = []  # No columns to remove

rename_tables_in_dataset(project_id, dataset_id, columns_to_remove)
