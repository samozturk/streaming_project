from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


PROJECT_ID = 'our-bebop-431708-i3'
DATASET_ID = 'bcn'
TABLE_ID = 'revised_raw'

df = pd.read_csv('../kafka_data/revised.csv',  engine='pyarrow')
df['time_ref'] = pd.to_datetime(df['time_ref'], format='%Y%m')
# Clean and validate the value column
df['value'] = pd.to_numeric(df['value'], errors='coerce')
df = df.dropna(subset=['value'])  # Remove rows where value is null
print(df.dtypes)
# df['time_ref'] = df['time_ref'].astype('str')


# Initialize BigQuery client
# credentials = service_account.Credentials.from_service_account_file('my-project.json')
# client = bigquery.Client(credentials=credentials, project='my-project-1531821615974')
client = bigquery.Client(project=PROJECT_ID)

# Create the table if it doesn't exist
table_ref = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}'
print(table_ref)
schema=[
        bigquery.SchemaField('time_ref', 'DATE'),
        bigquery.SchemaField('account', 'STRING'),
        bigquery.SchemaField('code', 'STRING'),
        bigquery.SchemaField('country_code', 'STRING'),
        bigquery.SchemaField('product_type', 'STRING'),
        bigquery.SchemaField('value', 'FLOAT'),
        bigquery.SchemaField('status', 'STRING')
    ]

table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table, exists_ok=True)

# Load DataFrame directly to BigQuery
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition='WRITE_TRUNCATE'
)
job = client.load_table_from_dataframe(
    df,
    table_ref,
    job_config=job_config
)

# Wait for the load job to complete
job.result()