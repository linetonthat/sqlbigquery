
# import libraries
from google.cloud import bigquery

# create a client object
client = bigquery.Client()

### --------------------------------------------------------------------------
### SELECT FROM
### --------------------------------------------------------------------------

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-data")
# https://openaq.org/#/?_k=esd1sb

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# construct a reference to the "global_air_quality" table
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
        
# Set up the query
query_job = client.query(query)        

# API request - run the query, and return a pandas DataFrame
us_cities = query_job.to_dataframe()

# What five cities have the most measurements?
us_cities['city'].value_counts().head()
us_cities.city.value_counts().head()

### --------------------------------------------------------------------------
# multiple selection
query = """
        SELECT city, country
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
 
### --------------------------------------------------------------------------       
# select all
query = """
        SELECT *
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'France'
        """        
### --------------------------------------------------------------------------       
# select distinct values
first_query = """
        SELECT DISTINCT country
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE unit = 'ppm'
        """
 
### --------------------------------------------------------------------------       
# working with big datasets
### -------------------------------------------------------------------------- 
query = """
        SELECT score, title
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = 'job'
        """ 
        
        
### -------------------------------------------------------------------------- 
### OPTION 1        
### --------------------------------------------------------------------------         
"""
Option 1: To begin,you can estimate the size of any query before running it. Here is an
 example using the (very large!) Hacker News dataset. To see how much data a query will scan,
 we create a
QueryJobConfig object and set the dry_run parameter to True .
"""        
# Create a QueryJobConfig object to estimate size of query without r unning it
dry_run_config = bigquery.QueryJobConfig(dry_run = True)  

# API request - dry run query to estimate costs
dry_run_query_job = client.query(query, job_config = dry_run_config)      

print("This query will process {} bytes.".format(dry_run_query_job.total_bytes_processed))

### -------------------------------------------------------------------------- 
### OPTION 2  
### --------------------------------------------------------------------------    
"""
Option 2: You can also specify a parameter when running the query to limit how much 
data you are willing to scan. Here's an example with a low limit.
"""  
# Only run the query if it's less than 100 MB
ONE_HUNDRED_MB = 100*1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_HUNDRED_MB)

# Set up the query (will only run if it's less than 100 MB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
safe_query_job.to_dataframe()

### -------------------------------------------------------------------------- 
# Only run the query if it's less than 1 Go
ONE_GB = 1000*1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_GB)

# Set up the query (will only run if it's less than 100 MB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
job_post_scores = safe_query_job.to_dataframe()

# Print average score for job posts
job_post_scores.score.mean()