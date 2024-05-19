from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def copy_tables_without_columns(project_id, source_dataset, destination_dataset, columns_to_remove):
    client = bigquery.Client(project=project_id)
    
    try:
        source_dataset_ref = client.dataset(source_dataset)
        tables = list(client.list_tables(source_dataset_ref))
        print(f"Found tables in source dataset {source_dataset}: {[table.table_id for table in tables]}")
        
        for table in tables:
            table_id = table.table_id
            source_table_ref = source_dataset_ref.table(table_id)
            source_table_obj = client.get_table(source_table_ref)
            all_columns = [schema_field.name for schema_field in source_table_obj.schema]
            
            # Keep only columns that are not in columns_to_remove
            columns_to_keep = [col for col in all_columns if col not in columns_to_remove]
            
            # Construct the SQL query
            query = f"""
            CREATE OR REPLACE TABLE `{project_id}.{destination_dataset}.{table_id}` AS
            SELECT 
              {', '.join(columns_to_keep)}
            FROM 
              `{project_id}.{source_dataset}.{table_id}`;
            """
            
            # Run the query
            job = client.query(query)
            job.result()  # Wait for the job to complete
            
            print(f"Copied table {table_id} to dataset {destination_dataset} without columns {', '.join(columns_to_remove)}")
    
    except NotFound as e:
        print(f"Error: {e}. Dataset {source_dataset} or {destination_dataset} not found in project {project_id}")


# Parameters
project_id = 'project_id' # Your project ID
source_dataset = 'source_dataset'  # The dataset you are copying from
destination_dataset = 'destination_dataset'  # The dataset you are copying to
columns_to_remove = ['column_to_remove1','column_to_remove2','column_to_remove3']  # Column to remove

copy_tables_without_columns(project_id, source_dataset, destination_dataset, columns_to_remove)


