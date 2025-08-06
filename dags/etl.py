from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json 


## Defining the DAG 
with DAG( 
    dag_id = "nasa_postges", 
    start_date = datetime.now() - timedelta(days=1),
    schedule = "@daily", 
    catchup = False,  
) as dag: 


    ### Create a table if it doesn't exists
    @task 
    def create_table(): 
        hook = PostgresHook(postgres_conn_id = "postgres_conn")
        # query for creation of the table. 
        hook.run("""
        CREATE TABLE IF NOT EXISTS apod (
            id SERIAL PRIMARY KEY, 
            title TEXT, 
            explanation TEXT, 
            url TEXT, 
            date DATE, 
            media_type TEXT, 
            hdurl TEXT, 
            service_version TEXT
        )""" )

    ## Extract the data from NASA API astronomy picture of the day it'll be extract pipeline 
    extract_data = HttpOperator( 
        task_id = "extract_apod_data", 
        http_conn_id = "nasa_api", 
        endpoint = "/planetary/apod", ## NASA API ENDPOINT
        method = "GET", 
        data = {"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"}, ## USING THE API KEY FROM CONNECTION 
        response_filter = lambda response: response.json(), # converting the response to json. 
    )

    ## Transform the data ( picking the information which we need to save from API in db ) 
    @task
    def transform_data(response): 
        # The NASA APOD API returns a dict, not a list under "data"
        # So we need to extract the fields and return as a list of tuples for insert_rows
        if not response:
            raise ValueError("No data found in the response")
        # Extract only the fields we want, in the correct order
        row = (
            response.get("title"),
            response.get("explanation"),
            response.get("url"),
            response.get("date"),
            response.get("media_type"),
            response.get("hdurl"),
            response.get("service_version"),
        )
        return [row]

    ## Loading the data into the postgres sequence 
    @task 
    def load_data(data): 
        hook = PostgresHook(postgres_conn_id = "postgres_conn")
        hook.insert_rows(table = "apod", rows = data, target_fields = ["title", "explanation", "url", "date", "media_type", "hdurl", "service_version"])
        return True


    ### Verify the data DBViewer 
    verify_data = PostgresHook( 
        task_id = "verify_data", 
        postgres_conn_id = "postgres_conn", 
        sql = "SELECT * FROM apod", 
    )



    ### Define the task dependencies 
    create_table() >> extract_data  ## ensuring the table is created before extraction of the data
    # extraction
    extracted_data = extract_data.output
    # transformation
    transformed_data = transform_data(extracted_data) ## transforming the data 
    # loading
    load_data(transformed_data) ## loading the data into the postgres db 
    # verification  
    verify_data  ## verifying the data in the db 