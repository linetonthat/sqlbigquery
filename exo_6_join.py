from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)


# List all the tables in the "stackoverflow" dataset
tables = list(client.list_tables(dataset))

# get list of tables
tables = [table.table_id for table in tables]  

# Construct a reference to the "posts_answers" table
answers_table_ref = dataset_ref.table("posts_answers")

# API request - fetch the table
answers_table = client.get_table(answers_table_ref)


# Preview the first five lines of the "posts_answers" table
client.list_rows(answers_table, max_results=5).to_dataframe()

# Construct a reference to the "posts_questions" table
questions_table_ref = dataset_ref.table("posts_questions")

# API request - fetch the table
questions_table = client.get_table(questions_table_ref)

# Preview the first five lines of the "posts_questions" table
client.list_rows(questions_table, max_results=5).to_dataframe()


# Construct the query
questions_query = """
                    SELECT id, title, owner_user_id
                    FROM `bigquery-public-data.stackoverflow.posts_questions` 
                    WHERE tags LIKE '%bigquery%'
                    """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 2 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=20**9)
questions_query_job = client.query(questions_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
questions = questions_query_job.to_dataframe() 




# Construct the query # check why it returns an error. Seems to come from FROM to parent_id
# is it linked to the tabs? Seems to come from a space put after "AS q" (how come it influences the result???)
answers_query = """
                SELECT a.id, a.body, a.owner_user_id
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                    ON q.id = a.parent_id
                WHERE q.tags LIKE '%bigquery%'
                """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
answers_query_job = client.query(answers_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
answers_results = answers_query_job.to_dataframe() 

"""
# here is a query that works.....
answers_query = """
                SELECT a.id, a.body, a.owner_user_id
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q 
                INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                    ON q.id = a.parent_id
                WHERE q.tags LIKE '%bigquery%'
                """

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
answers_query_job = client.query(answers_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
answers_results = answers_query_job.to_dataframe()
"""

# Careful with tab: should align with """?
# Need to group by a.owner_user_id instead of user_id?
# why is it on ON q.id = a.parent_Id (wih Capital I) and not ON q.id = a.parent_id?
bigquery_experts_query = """
                         SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                         FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                         INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                             ON q.id = a.parent_id
                         WHERE q.tags LIKE '%bigquery%'
                         GROUP BY a.owner_user_id
                         """
        
# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
bigquery_experts_query_job = client.query(bigquery_experts_query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
bigquery_experts_results = bigquery_experts_query_job.to_dataframe() 

       
# get topic                  
topic = 'python'

# define function (and sort by descending number of answers)
def list_experts(topic):                      
    # Construct the query  
    topic_experts_query = """
                             SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                             FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                             INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                                 ON q.id = a.parent_Id
                             WHERE q.tags LIKE '%{}%'
                             GROUP BY a.owner_user_id
                             ORDER BY number_of_answers DESC
                             """.format(topic)
            
    # Set up the query (cancel the query if it would use too much of 
    # your quota, with the limit set to 2 GB)
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=20**9)
    topic_experts_query_job = client.query(topic_experts_query, job_config=safe_config)
    
    # API request - run the query, and convert the results to a pandas DataFrame
    topic_experts_results = topic_experts_query_job.to_dataframe() 
    
    
    # Preview results
    print(topic_experts_results.head())

# get results
list_experts(topic)


"""
# code in the solution...
bigquery_experts_query = """
                         SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                         FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                         INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                             ON q.id = a.parent_Id
                         WHERE q.tags LIKE '%bigquery%'
                         GROUP BY a.owner_user_id
                         """
"""
