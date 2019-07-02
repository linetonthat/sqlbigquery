from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "github_repos" dataset
dataset_ref = client.dataset("github_repos", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "licenses" table
licenses_ref = dataset_ref.table("licenses")

# API request - fetch the table
licenses_table = client.get_table(licenses_ref)

# Preview the first five lines of the "licenses" table
client.list_rows(licenses_table, max_results=5).to_dataframe()


# Construct a reference to the "sample_files" table
files_ref = dataset_ref.table("sample_files")

# API request - fetch the table
files_table = client.get_table(files_ref)

# Preview the first five lines of the "sample_files" table
client.list_rows(files_table, max_results=5).to_dataframe()


# Construct the query
file_count_by_license_query = """
                        SELECT l.license AS license, COUNT(1) AS num_files
                        FROM `bigquery-public-data.github_repos.licenses` AS l 
                        INNER JOIN `bigquery-public-data.github_repos.sample_files` AS s
                            ON l.repo_name = s.repo_name
                        GROUP BY license
                        ORDER BY num_files DESC
                        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
file_count_by_license_query_job = client.query(file_count_by_license_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
file_count_by_license = file_count_by_license_query_job.to_dataframe() 


       