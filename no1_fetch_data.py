
# import libraries
from google.cloud import bigquery

# create a client object
client = bigquery.Client()

### --------------------------------------------------------------------------
### Fetch data from bigquery-public-data
### --------------------------------------------------------------------------
# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the "hacker_news" dataset
tables = list(client.list_tables(dataset))

# print list of tables
for table in tables:
    print(table.table_id)
    
# construct a reference to the "full" table
table_ref = dataset_ref.table("full")

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns in the "full" table in the "hacker_news" dataset
table.schema

# Preview the first five lines of the "full" table
client.list_rows(table, max_results = 5).to_dataframe()

### --------------------------------------------------------------------------
### SELECT FROM
### --------------------------------------------------------------------------

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-data")
# https://openaq.org/#/?_k=esd1sb

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# construct a reference to the "full" table
table_ref = dataset_ref.table("global_air_quality")

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns in the "full" table in the "hacker_news" dataset
table.schema

# Print information on all the columns in the "full" table in the "hacker_news" dataset
client.list_rows(table, max_results = 5).to_dataframe()


# query
query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
        
        