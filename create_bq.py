from google.cloud import bigquery

def create_bigquery_table(table_id ,schema):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    table_schema = []
    for field in schema:
        field_name = field['name']
        field_type = field['type']
        field_mode = field['mode']
        table_schema.append(bigquery.SchemaField(field_name, field_type, mode=field_mode))

    table = bigquery.Table(table_id, schema=table_schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="join_date",  # Specify the column to use for partitioning
        expiration_ms=None   # No expiration
    )
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

# Replace these variables with your project and dataset details
project_id = "your-project-id"
dataset_id = "your-dataset-id"
table_name = "new_table_name"

table_id = project_id+'.'+dataset_id+'.'+table_name

# Define your schema
schema = [
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "age", "type": "INT64", "mode": "REQUIRED"},
    {"name": "salary", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "is_active", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "join_date", "type": "DATE", "mode": "REQUIRED"},
]

# Call the function to create the table
create_bigquery_table(table_id, schema)
