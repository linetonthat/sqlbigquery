from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "chicago_taxi_trips" dataset
dataset_ref = client.dataset("chicago_taxi_trips", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the "chicago_taxi_trips" dataset
tables = list(client.list_tables(dataset))

# print list of tables
for table in tables:
    print(table.table_id)

# Construct a reference to the "taxi_trips" table
table_ref = dataset_ref.table("taxi_trips")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "taxi_trips" table
taxi_trips = client.list_rows(table, max_results=5).to_dataframe()

taxi_trips.info()

### --------------------------------------------------------------------------
### RIDES PER YEAR
### --------------------------------------------------------------------------

# Query to select the number of rides per year, sorted by year
rides_per_year_query = """
                        SELECT EXTRACT(YEAR FROM trip_start_timestamp) AS year,
                                     COUNT(1) AS num_trips
                        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                        GROUP BY year
                        ORDER BY year
                        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
rides_per_year_query_job = client.query(rides_per_year_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
rides_per_year_result = rides_per_year_query_job.to_dataframe()

### --------------------------------------------------------------------------
### RIDES PER MONTH
### --------------------------------------------------------------------------

# Query to select the number of rides per month in 2017
rides_per_month_query = """                       
                        SELECT EXTRACT(MONTH FROM trip_start_timestamp) AS month,
                            COUNT(1) AS num_trips
                        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                        WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2017
                        GROUP BY month
                        ORDER BY month
                        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
rides_per_month_query_job = client.query(rides_per_month_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
rides_per_month_result = rides_per_month_query_job.to_dataframe()

### --------------------------------------------------------------------------
### SPEED
### --------------------------------------------------------------------------

# Query to assess the average speed per hour of day in H1 of 2017
speeds_query = """
                WITH RelevantRides AS
                    (
                    SELECT trip_start_timestamp,
                        trip_miles,
                        trip_seconds
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2017
                        AND EXTRACT(MONTH FROM trip_start_timestamp) >=1
                        AND EXTRACT(MONTH FROM trip_start_timestamp) <= 6
                        AND trip_seconds > 0
                        AND trip_miles > 0
                    )                      
                SELECT EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day,
                        3600 * SUM(trip_miles) / SUM(trip_seconds) AS avg_mph,
                        COUNT(1) AS num_trips
                FROM RelevantRides
                GROUP BY hour_of_day
                ORDER BY hour_of_day
                        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
speeds_query_job = client.query(speeds_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
speeds_result = speeds_query_job.to_dataframe()

### --------------------------------------------------------------------------
### SPEED - ANSWER
### --------------------------------------------------------------------------

speeds_query = """
               WITH RelevantRides AS
               (
                   SELECT EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day, 
                          trip_miles, 
                          trip_seconds
                   FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                   WHERE trip_start_timestamp > '2017-01-01' AND 
                         trip_start_timestamp < '2017-07-01' AND 
                         trip_seconds > 0 AND 
                         trip_miles > 0
               )
               SELECT hour_of_day, 
                      COUNT(1) AS num_trips, 
                      3600 * SUM(trip_miles) / SUM(trip_seconds) AS avg_mph
               FROM RelevantRides
               GROUP BY hour_of_day
               ORDER BY hour_of_day
               """

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
speeds_query_job = client.query(speeds_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
speeds_result = speeds_query_job.to_dataframe()

# View results
print(speeds_result)



### --------------------------------------------------------------------------
### SPEED
### --------------------------------------------------------------------------

# Query to assess the max trip miles and min trip duration per hour of day in H1 of 2017
speeds_investigation_query = """
                WITH RelevantRides AS
                    (
                    SELECT trip_start_timestamp,
                        trip_miles,
                        trip_seconds
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2017
                        AND EXTRACT(MONTH FROM trip_start_timestamp) >=1
                        AND EXTRACT(MONTH FROM trip_start_timestamp) <= 6
                        AND trip_seconds > 0
                        AND trip_miles > 0
                    )                      
                SELECT EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day,
                        MAX(trip_miles) AS max_trip_miles,
                        MIN(trip_seconds) AS min_trip_seconds,
                        COUNT(1) AS num_trips
                FROM RelevantRides
                GROUP BY hour_of_day
                ORDER BY hour_of_day
                        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
speeds_investigation_query_job = client.query(speeds_investigation_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
speeds_investigation_result = speeds_investigation_query_job.to_dataframe()


### --------------------------------------------------------------------------

# Query to assess the average trip miles and average trip duration per hour of day in H1 of 2017
speeds_investigation_query = """
                WITH RelevantRides AS
                    (
                    SELECT trip_start_timestamp,
                        trip_miles,
                        trip_seconds
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2017
                        AND EXTRACT(MONTH FROM trip_start_timestamp) >=1
                        AND EXTRACT(MONTH FROM trip_start_timestamp) <= 6
                        AND trip_seconds > 0
                        AND trip_miles > 0
                    )                      
                SELECT EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day,
                        AVG(trip_miles) AS avg_trip_miles,
                        AVG(trip_seconds) AS avg_trip_seconds,
                        COUNT(1) AS num_trips
                FROM RelevantRides
                GROUP BY hour_of_day
                ORDER BY hour_of_day
                        """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
speeds_investigation_query_job = client.query(speeds_investigation_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
speeds_investigation_result = speeds_investigation_query_job.to_dataframe()
