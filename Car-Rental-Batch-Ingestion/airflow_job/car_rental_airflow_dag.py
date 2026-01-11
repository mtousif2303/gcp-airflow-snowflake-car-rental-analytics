from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# ==============================================================================
# DAG DEFAULT ARGUMENTS
# ==============================================================================
# Default settings applied to all tasks in this DAG
default_args = {
    'owner': 'airflow',                    # Owner of the DAG
    'depends_on_past': False,              # Tasks don't depend on previous runs
    'email_on_failure': False,             # Don't send email on task failure
    'email_on_retry': False,               # Don't send email on retry
    'retries': 1,                          # Retry failed tasks once
    'retry_delay': timedelta(minutes=5),   # Wait 5 minutes before retry
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
# Main DAG configuration for the car rental data pipeline
dag = DAG(
    'car_rental_data_pipeline',                           # Unique DAG identifier
    default_args=default_args,                            # Apply default args to all tasks
    description='Car Rental Data Pipeline',               # Human-readable description
    schedule_interval=None,                               # Manual trigger only (no automatic scheduling)
    start_date=datetime(2025, 9, 3),                      # DAG becomes active from this date
    catchup=False,                                        # Don't backfill historical runs
    tags=['dev'],                                         # Tags for filtering in Airflow UI
    params={
        # Allow users to pass execution_date parameter when triggering DAG manually
        'execution_date': Param(
            default='NA', 
            type='string', 
            description='Execution date in yyyymmdd format'
        ),
    }
)

# ==============================================================================
# PYTHON CALLABLE: GET EXECUTION DATE
# ==============================================================================
def get_execution_date(ds_nodash, **kwargs):
    """
    Determines the effective execution date for this pipeline run.
    
    Logic:
    - If user provides 'execution_date' parameter, use that value
    - Otherwise, fall back to Airflow's ds_nodash (execution date without dashes)
    
    Args:
        ds_nodash: Airflow's execution date in YYYYMMDD format
        **kwargs: Contains params dict with user-provided parameters
    
    Returns:
        str: Execution date in YYYYMMDD format
    """
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# ==============================================================================
# TASK 1: RESOLVE EXECUTION DATE
# ==============================================================================
# This task determines which date to process and stores it in XCom for downstream tasks
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',                         # Task identifier
    python_callable=get_execution_date,                   # Function to execute
    provide_context=True,                                 # Pass Airflow context to function
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},          # Pass execution date template
    dag=dag,
)

# ==============================================================================
# TASK 2: MERGE CUSTOMER DIMENSION (SCD TYPE 2 - CLOSE OLD RECORDS)
# ==============================================================================
# This task implements Slowly Changing Dimension Type 2 logic
# It closes out existing records that have changed by:
# 1. Setting end_date to current timestamp
# 2. Setting is_current flag to FALSE
merge_customer_dim = SQLExecuteQueryOperator(
    task_id='merge_customer_dim',
    conn_id='snowflake-connection',                       # Snowflake connection ID from Airflow
    sql="""
        -- Set database and schema context to ensure file format is found
        USE DATABASE car_rental;
        USE SCHEMA PUBLIC;
        
        -- MERGE statement to update existing customer records that have changed
        MERGE INTO car_rental.PUBLIC.customer_dim AS target
        USING (
            -- Read CSV file from Snowflake stage
            -- File name is dynamically constructed using execution date from XCom
            SELECT
                $1 AS customer_id,                        -- Column 1 from CSV
                $2 AS name,                               -- Column 2 from CSV
                $3 AS email,                              -- Column 3 from CSV
                $4 AS phone                               -- Column 4 from CSV
            FROM @car_rental.PUBLIC.car_rental_data_stg/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv 
            (FILE_FORMAT => CSV_FORMAT)                   -- Use pre-defined CSV file format
        ) AS source
        ON target.customer_id = source.customer_id        -- Match on customer ID
           AND target.is_current = TRUE                   -- Only match current records
        WHEN MATCHED AND (
            -- Detect changes in any customer attribute
            target.name != source.name OR
            target.email != source.email OR
            target.phone != source.phone
        ) THEN
            -- Close out the old record (SCD Type 2)
            UPDATE SET 
                target.end_date = CURRENT_TIMESTAMP(),    -- Mark when record became invalid
                target.is_current = FALSE;                -- Mark as no longer current
    """,
    dag=dag,
)

# ==============================================================================
# TASK 3: INSERT NEW CUSTOMER DIMENSION RECORDS (SCD TYPE 2 - ADD NEW VERSIONS)
# ==============================================================================
# This task inserts new customer records:
# 1. Brand new customers (never seen before)
# 2. New versions of existing customers (after their old version was closed in Task 2)
insert_customer_dim = SQLExecuteQueryOperator(
    task_id='insert_customer_dim',
    conn_id='snowflake-connection',
    sql="""
        -- Set database and schema context
        USE DATABASE car_rental;
        USE SCHEMA PUBLIC;
        
        -- Insert all customer records from the CSV file
        -- This includes both new customers and new versions of existing customers
        INSERT INTO car_rental.PUBLIC.customer_dim (
            customer_id, 
            name, 
            email, 
            phone, 
            effective_date,                               -- When this record version became valid
            end_date,                                     -- When this version becomes invalid (NULL = current)
            is_current                                    -- Flag indicating current record
        )
        SELECT
            $1 AS customer_id,
            $2 AS name,
            $3 AS email,
            $4 AS phone,
            CURRENT_TIMESTAMP() AS effective_date,        -- Record is effective now
            NULL AS end_date,                             -- NULL means record is still current
            TRUE AS is_current                            -- Mark as current version
        FROM @car_rental.PUBLIC.car_rental_data_stg/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv 
        (FILE_FORMAT => CSV_FORMAT);
    """,
    dag=dag,
)

# ==============================================================================
# DATAPROC JOB CONFIGURATION
# ==============================================================================
# Configuration for Google Cloud Dataproc cluster and PySpark job
CLUSTER_NAME = 'hadoop-spark-cluster'                     # Existing Dataproc cluster name
PROJECT_ID = 'project-040088dd-8c9a-464e-96f'             # GCP Project ID
REGION = 'us-central1'                                    # GCP region where cluster exists

# Path to the PySpark job script in Google Cloud Storage
pyspark_job_file_path = 'gs://snowflake-projects--gds-de/car_rental_spark_job/spark_job.py'

# PySpark job configuration for Dataproc
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": pyspark_job_file_path,    # Main PySpark script to execute
        "args": [
            # Pass execution date to Spark job as command-line argument
            "--date={{ ti.xcom_pull(task_ids='get_execution_date') }}"
        ],
        "jar_file_uris": [
            # Snowflake Spark connector JAR
            "gs://snowflake-projects--gds-de/snowflake_jars/spark-snowflake_2.12-2.15.0-spark_3.4.jar",
            # Snowflake JDBC driver JAR
            "gs://snowflake-projects--gds-de/snowflake_jars/snowflake-jdbc-3.16.0.jar"
        ]
    }
}

# ==============================================================================
# TASK 4: SUBMIT PYSPARK JOB TO DATAPROC
# ==============================================================================
# This task submits the PySpark job to process rental transactions
# The Spark job reads from staging, performs transformations, and loads to fact tables
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=PYSPARK_JOB,                                      # Job configuration defined above
    region=REGION,                                        # GCP region
    project_id=PROJECT_ID,                                # GCP project
    dag=dag,
)

# ==============================================================================
# TASK ORCHESTRATION / DEPENDENCIES
# ==============================================================================
# Define the execution order of tasks:
# 1. Get execution date from params or use default
# 2. Close out changed customer records (SCD Type 2)
# 3. Insert new/updated customer records as current versions
# 4. Submit PySpark job to process rental transactions
get_execution_date_task >> merge_customer_dim >> insert_customer_dim >> submit_pyspark_job