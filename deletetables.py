from google.cloud import bigquery

def delete_new_tables(project_id, datasets):
    client = bigquery.Client(project=project_id)
    
    for dataset_id in datasets:
        dataset_ref = client.dataset(dataset_id)
        tables = list(client.list_tables(dataset_ref))
        
        for table in tables:
            table_id = table.table_id
            if '_new' in table_id:
                table_ref = dataset_ref.table(table_id)
                client.delete_table(table_ref)
                print(f"Deleted table {table_id} in dataset {dataset_id}")

# Parameters
project_id = 'sun-peaks-grand'
datasets = ['client_universal_analytics']  # List all your datasets

# Delete tables with '_new' in their names
delete_new_tables(project_id, datasets)
