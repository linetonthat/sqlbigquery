
# import libraries
from google.cloud import bigquery

# create a client object
client = bigquery.Client()

### --------------------------------------------------------------------------
### SELECT FROM
### --------------------------------------------------------------------------

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")
# https://openaq.org/#/?_k=esd1sb

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# construct a reference to the "comments" table
table_ref = dataset_ref.table("comments")

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns in the "full" table in the "hacker_news" dataset
table.schema

# Print information on all the columns in the "full" table in the "hacker_news" dataset
client.list_rows(table, max_results = 5).to_dataframe()


# query
query = """
        SELECT parent, COUNT(id)
        FROM `bigquery-public-data.hacker_news.comments`
        GROUP BY parent
        HAVING COUNT(id) > 10
        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
popular_comments = query_job.to_dataframe()

# Print the first five rows of the DataFrame
popular_comments.head()

### --------------------------------------------------------------------------
### ALIASING AND OTHER IMPROVEMENTS
### --------------------------------------------------------------------------

# query
query = """
        SELECT parent, COUNT(1) AS NumPosts
        FROM `bigquery-public-data.hacker_news.comments`
        GROUP BY parent
        HAVING COUNT(1) > 10
        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
popular_comments = query_job.to_dataframe()

# Print the first five rows of the DataFrame
popular_comments.head()