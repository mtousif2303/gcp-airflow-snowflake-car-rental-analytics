# gcp-airflow-snowflake-car-rental-analytics

End-to-end data pipeline built with Apache Airflow, Apache Spark (GCP Dataproc), and Snowflake. Processes daily car rental transactions, implements SCD Type 2 for historical tracking, and creates analytics-ready dimensional models for business intelligence.

## 🚗 Car Rental Batch Ingestion

This project demonstrates a comprehensive **batch data processing pipeline** for car rental analytics using **Apache Airflow**, **Google Cloud Dataproc**, **Apache Spark**, and **Snowflake**. The pipeline processes daily car rent

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   GCS Storage   │───▶│  Apache Airflow  │───▶│ Google Dataproc │───▶│   Snowflake     │
│                 │    │                  │    │                 │    │                 │
│ • JSON Files    │    │ • DAG Orchestr.  │    │ • Spark Jobs    │    │ • Data Warehouse│
│ • CSV Files     │    │ • SCD2 Logic     │    │ • Data Transform│    │ • Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🎯 Key Features

- **🔄 Batch Processing**: Daily data ingestion with parameterized execution dates
- **📊 SCD Type 2**: Slowly Changing Dimension implementation for customer data
- **⚡ Spark Processing**: Large-scale data transformation using PySpark
- **🎛️ Airflow Orchestration**: Workflow management with dependency handling
- **☁️ Cloud Integration**: Google Cloud Storage and Dataproc integration
- **❄️ Snowflake Analytics**: Data warehouse with dimensional modeling

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Google Cloud Storage                        │
│  ┌──────────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │  Raw JSON Data   │  │  Spark Job   │  │  Snowflake JARs │  │
│  └────────┬─────────┘  └──────┬───────┘  └────────┬────────┘  │
└───────────┼────────────────────┼──────────────────┼────────────┘
            │                    │                   │
            ▼                    ▼                   ▼
    ┌───────────────────────────────────────────────────┐
    │            Apache Airflow (Orchestration)         │
    │  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
    │  │ Get Date │→ │ Merge    │→ │ Insert        │  │
    │  │          │  │ Customer │  │ Customer      │  │
    │  └──────────┘  └──────────┘  └───────┬───────┘  │
    │                                       │          │
    │                          ┌────────────▼───────┐  │
    │                          │ Submit PySpark Job │  │
    │                          └────────┬───────────┘  │
    └──────────────────────────────────┼──────────────┘
                                       │
                                       ▼
                        ┌──────────────────────────┐
                        │   Dataproc Cluster       │
                        │   (Spark Processing)     │
                        └──────────┬───────────────┘
                                   │
                                   ▼
                        ┌──────────────────────────┐
                        │   Snowflake DWH          │
                        │  ┌──────────────────┐    │
                        │  │ Dimension Tables │    │
                        │  │  - Customer      │    │
                        │  │  - Car           │    │
                        │  │  - Location      │    │
                        │  │  - Date          │    │
                        │  └─────────┬────────┘    │
                        │            │             │
                        │  ┌─────────▼────────┐    │
                        │  │  Rentals Fact    │    │
                        │  └──────────────────┘    │
                        └──────────────────────────┘
```

---

This guide demonstrates an end-to-end data engineering pipeline that processes daily car rental transactions using Apache Airflow, Google Cloud Dataproc (Spark), and Snowflake. The pipeline implements Slowly Changing Dimensions (SCD Type 2) for customer data and creates a star schema for analytics.


The Airflow Dag pipeline

<img width="2886" height="1750" alt="image" src="https://github.com/user-attachments/assets/60836f94-9332-450d-aebf-3f43f13047ac" />

a) GCS Bucket data and spark Job 

<img width="3030" height="1756" alt="image" src="https://github.com/user-attachments/assets/1514a95c-6b3d-4d87-b8d6-52131ed30f20" />

b) Upload the Jar files 

<img width="3094" height="1606" alt="image" src="https://github.com/user-attachments/assets/ce5addb2-f766-4488-98a0-90ebbfb8d0f3" />

c) Then upload the spark Job

<img width="3110" height="1576" alt="image" src="https://github.com/user-attachments/assets/25ae8141-5d8b-40fb-a40e-e3c0f0c5b5c1" />

d) The the upload the car_rental_airflow_dag.py 

<img width="3082" height="1702" alt="image" src="https://github.com/user-attachments/assets/52f7cc2f-65ff-4995-8e87-66df0a3f5840" />

e) Snowflake data warehouse the tables are merged and created the rent fact table

<img width="2948" height="1760" alt="image" src="https://github.com/user-attachments/assets/64c64f11-c000-44b5-81ed-447d8ac3fa61" />


## 📦 Step A: Organize GCS Bucket Structure

### Purpose
Google Cloud Storage serves as the data lake for raw input files, processing scripts, and required dependencies.

### Folder Structure

```
gs://snowflake-projects--gds-de/
│
├── 📁 car_rental_data/
│   └── 📁 car_rental_daily_data/
│       ├── 📄 car_rental_20260110.json
│       ├── 📄 car_rental_20260111.json
│       ├── 📄 car_rental_20260112.json
│       └── 📄 ... (daily incremental files)
│
├── 📁 car_rental_spark_job/
│   └── 📄 spark_job.py
│
└── 📁 snowflake_jars/
    ├── 📄 spark-snowflake_2.12-2.15.0-spark_3.4.jar
    └── 📄 snowflake-jdbc-3.16.0.jar
```

### Implementation Steps

**1. Create the bucket (if not exists):**
```bash
gsutil mb -p project-040088dd-8c9a-464e-96f \
  -c STANDARD \
  -l us-central1 \
  gs://snowflake-projects--gds-de/
```

**2. Create folder structure:**
```bash
# Create directories
gsutil mkdir gs://snowflake-projects--gds-de/car_rental_data/
gsutil mkdir gs://snowflake-projects--gds-de/car_rental_data/car_rental_daily_data/
gsutil mkdir gs://snowflake-projects--gds-de/car_rental_spark_job/
gsutil mkdir gs://snowflake-projects--gds-de/snowflake_jars/
```

**3. Upload sample data file:**
```bash
# Upload your daily JSON file
gsutil cp car_rental_20260111.json \
  gs://snowflake-projects--gds-de/car_rental_data/car_rental_daily_data/
```

### Raw Data Format

**File:** `car_rental_20260111.json`
```json
[
  {
    "rental_id": "R20260111001",
    "customer_id": "C1001",
    "car": {
      "make": "Toyota",
      "model": "Camry",
      "year": 2023
    },
    "rental_period": {
      "start_date": "2026-01-11",
      "end_date": "2026-01-15"
    },
    "rental_location": {
      "pickup_location": "LAX Airport",
      "dropoff_location": "LAX Airport"
    },
    "amount": 89.99,
    "quantity": 1
  },
  {
    "rental_id": "R20260111002",
    "customer_id": "C1002",
    "car": {
      "make": "Honda",
      "model": "Accord",
      "year": 2024
    },
    "rental_period": {
      "start_date": "2026-01-11",
      "end_date": "2026-01-18"
    },
    "rental_location": {
      "pickup_location": "Downtown LA",
      "dropoff_location": "LAX Airport"
    },
    "amount": 79.99,
    "quantity": 1
  }
]
```

### Best Practices
- **Naming Convention**: Use `YYYYMMDD` format for date-based partitioning
- **File Format**: JSON multiline array for easy Spark processing
- **Incremental Loading**: Each file represents one day's transactions
- **Data Validation**: Ensure all mandatory fields are present before upload

---

## 🔧 Step B: Upload Snowflake Connector JARs

### Purpose
These JAR files enable Apache Spark to communicate with Snowflake, allowing seamless data reading and writing operations.

### Required Dependencies

#### 1. Spark-Snowflake Connector
- **File**: `spark-snowflake_2.12-2.15.0-spark_3.4.jar`
- **Version**: 2.15.0 (compiled for Scala 2.12, Spark 3.4)
- **Purpose**: Provides Spark DataSource API implementation for Snowflake
- **Download**: [Maven Repository](https://mvnrepository.com/artifact/net.snowflake/spark-snowflake)

#### 2. Snowflake JDBC Driver
- **File**: `snowflake-jdbc-3.16.0.jar`
- **Version**: 3.16.0 (recommended: 3.14.4 for production)
- **Purpose**: Underlying JDBC driver for database connectivity
- **Download**: [Maven Repository](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc)

### Upload Commands

```bash
# Navigate to your local directory containing the JARs
cd ~/downloads/snowflake-jars/

# Upload Spark-Snowflake connector
gsutil cp spark-snowflake_2.12-2.15.0-spark_3.4.jar \
  gs://snowflake-projects--gds-de/snowflake_jars/

# Upload Snowflake JDBC driver
gsutil cp snowflake-jdbc-3.16.0.jar \
  gs://snowflake-projects--gds-de/snowflake_jars/
```

### Verify Upload

```bash
# List files in the jars directory
gsutil ls -lh gs://snowflake-projects--gds-de/snowflake_jars/

# Expected output:
# 42.5 MiB  spark-snowflake_2.12-2.15.0-spark_3.4.jar
# 28.3 MiB  snowflake-jdbc-3.16.0.jar
```

### Important Notes
- **Version Compatibility**: Ensure Spark version (3.4) matches your Dataproc cluster
- **Scala Version**: The `2.12` in the JAR name must match your Spark's Scala version
- **JDBC Warning**: The pipeline may warn about JDBC version mismatch (3.16.0 vs 3.14.4), but it will still work
- **Security**: Store JARs in a restricted GCS bucket with appropriate IAM permissions

---

## 🚀 Step C: Upload PySpark Job Script

### Purpose
The PySpark job performs the core ETL transformations: reading raw JSON, validating data, calculating business metrics, joining with dimension tables, and loading the fact table into Snowflake.

### Script Overview

**File**: `spark_job.py`
**Location**: `gs://snowflake-projects--gds-de/car_rental_spark_job/`

### Key Functionalities

1. **Data Ingestion**: Read daily JSON files from GCS
2. **Data Validation**: Filter out records with missing mandatory fields
3. **Business Transformations**:
   - Calculate rental duration in days
   - Compute total rental amount
   - Calculate average daily rental rate
   - Flag long-term rentals (> 7 days)
4. **Dimension Lookups**: Join with dimension tables to get surrogate keys
5. **Fact Table Load**: Write transformed data to Snowflake

### Upload Process

```bash
# Navigate to your local directory
cd ~/projects/car-rental-pipeline/

# Upload the PySpark script
gsutil cp spark_job.py \
  gs://snowflake-projects--gds-de/car_rental_spark_job/

# Verify upload
gsutil cat gs://snowflake-projects--gds-de/car_rental_spark_job/spark_job.py | head -20
```

### Script Highlights

**Snowflake Connection Configuration:**
```python
snowflake_options = {
    "sfURL": "https://essbbdc-mi10939.snowflakecomputing.com",
    "sfAccount": "ESSBBDC-MI10939",
    "sfUser": os.environ.get("SNOWFLAKE_USER"),
    "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
    "sfDatabase": "car_rental",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}
```

**Business Transformations:**
```python
# Calculate rental duration and derived metrics
transformed_df = validated_df \
    .withColumn("rental_duration_days", 
                datediff(col("rental_period.end_date"), 
                        col("rental_period.start_date"))) \
    .withColumn("total_rental_amount", 
                col("amount") * col("quantity")) \
    .withColumn("average_daily_rental_amount", 
                round(col("total_rental_amount") / col("rental_duration_days"), 2)) \
    .withColumn("is_long_rental", 
                when(col("rental_duration_days") > 7, lit(1)).otherwise(lit(0)))
```

### Security Recommendations

**Option 1: Use Environment Variables (via Dataproc job properties)**
```python
import os
"sfUser": os.environ.get("SNOWFLAKE_USER"),
"sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
```

**Option 2: Use Google Secret Manager**
```python
from google.cloud import secretmanager

def get_secret(secret_id, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

"sfUser": get_secret("snowflake-user", "project-040088dd-8c9a-464e-96f"),
"sfPassword": get_secret("snowflake-password", "project-040088dd-8c9a-464e-96f"),
```

### Testing the Script Locally (Optional)

```bash
# Install required packages
pip install pyspark==3.4.0

# Run with sample data
python spark_job.py --date=20260111
```

---

## 📊 Step D: Deploy Airflow DAG

### Purpose
The Airflow DAG orchestrates the entire pipeline: resolving execution dates, performing SCD Type 2 updates on customer dimensions, and triggering the Spark job on Dataproc.

### DAG Overview

**File**: `car_rental_airflow_dag.py`
**Location**: Airflow DAGs folder (typically `gs://[composer-bucket]/dags/`)

### DAG Architecture

```
┌─────────────────────────────────────────────────────┐
│              car_rental_data_pipeline               │
├─────────────────────────────────────────────────────┤
│                                                     │
│  1. get_execution_date (PythonOperator)            │
│     ↓                                               │
│     • Resolve date from params or use ds_nodash    │
│     • Store in XCom for downstream tasks           │
│                                                     │
│  2. merge_customer_dim (SQLExecuteQueryOperator)    │
│     ↓                                               │
│     • Close out changed customer records           │
│     • Set end_date and is_current=FALSE            │
│                                                     │
│  3. insert_customer_dim (SQLExecuteQueryOperator)   │
│     ↓                                               │
│     • Insert new customer versions                 │
│     • Set is_current=TRUE, end_date=NULL           │
│                                                     │
│  4. submit_pyspark_job (DataprocSubmitJobOperator)  │
│     ↓                                               │
│     • Submit Spark job to Dataproc cluster         │
│     • Pass execution date as argument              │
│     • Load fact table to Snowflake                 │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Upload Process

**For Cloud Composer:**
```bash
# Get your Composer environment's bucket
export COMPOSER_BUCKET=$(gcloud composer environments describe \
  car-rental-composer \
  --location=us-central1 \
  --format="get(config.dagGcsPrefix)")

# Upload the DAG
gsutil cp car_rental_airflow_dag.py ${COMPOSER_BUCKET}/

# Verify upload
gsutil ls ${COMPOSER_BUCKET}/*.py
```

**For Self-Managed Airflow:**
```bash
# Copy to Airflow DAGs folder
cp car_rental_airflow_dag.py /opt/airflow/dags/

# Or via GCS if using GKE
gsutil cp car_rental_airflow_dag.py gs://your-airflow-bucket/dags/
```

### DAG Configuration

**Key Parameters:**
```python
dag = DAG(
    'car_rental_data_pipeline',
    default_args=default_args,
    description='Car Rental Data Pipeline',
    schedule_interval=None,              # Manual trigger only
    start_date=datetime(2025, 9, 3),
    catchup=False,                       # Don't backfill
    tags=['dev', 'car_rental'],
    params={
        'execution_date': Param(
            default='NA',
            type='string',
            description='Execution date in YYYYMMDD format'
        ),
    }
)
```

**Snowflake Connection Setup:**
```python
# In Airflow UI: Admin → Connections
Connection ID: snowflake-connection
Connection Type: Snowflake
Account: ESSBBDC-MI10939
Database: car_rental
Schema: PUBLIC
Warehouse: COMPUTE_WH
Role: ACCOUNTADMIN
```

**Dataproc Configuration:**
```python
CLUSTER_NAME = 'hadoop-spark-cluster'
PROJECT_ID = 'project-040088dd-8c9a-464e-96f'
REGION = 'us-central1'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": pyspark_job_file_path,
        "args": ["--date={{ ti.xcom_pull(task_ids='get_execution_date') }}"],
        "jar_file_uris": [
            "gs://snowflake-projects--gds-de/snowflake_jars/spark-snowflake_2.12-2.15.0-spark_3.4.jar",
            "gs://snowflake-projects--gds-de/snowflake_jars/snowflake-jdbc-3.16.0.jar"
        ],
        "properties": {
            "spark.executorEnv.SNOWFLAKE_USER": "your_username",
            "spark.executorEnv.SNOWFLAKE_PASSWORD": "your_password"
        }
    }
}
```

### Testing the DAG

**1. Validate DAG syntax:**
```bash
# SSH into Airflow scheduler
airflow dags list | grep car_rental

# Check for import errors
airflow dags list-import-errors
```

**2. Test individual tasks:**
```bash
# Test date resolution
airflow tasks test car_rental_data_pipeline get_execution_date 2026-01-11

# Test Snowflake merge
airflow tasks test car_rental_data_pipeline merge_customer_dim 2026-01-11

# Test Snowflake insert
airflow tasks test car_rental_data_pipeline insert_customer_dim 2026-01-11
```

**3. Trigger DAG manually:**
- Navigate to Airflow UI: `http://[your-airflow-url]:8080`
- Find `car_rental_data_pipeline`
- Click "Trigger DAG" → Enter `execution_date` parameter → Trigger

### Monitoring & Troubleshooting

**Check Task Logs:**
```bash
# View task logs
airflow tasks logs car_rental_data_pipeline submit_pyspark_job 2026-01-11 1

# Monitor DAG run
airflow dags state car_rental_data_pipeline 2026-01-11
```

**Common Issues:**

1. **Snowflake Connection Error**
   - Verify connection in Airflow UI
   - Check warehouse is running
   - Verify network connectivity (Cloud NAT)

2. **XCom Pull Fails**
   - Ensure `get_execution_date` task completed successfully
   - Check XCom values in Airflow UI

3. **Dataproc Job Timeout**
   - Check cluster is running
   - Verify JAR files exist in GCS
   - Review Dataproc job logs in GCP Console

---

## 🎯 Step E: Verify Snowflake Results

### Purpose
Validate that the pipeline successfully loaded data into Snowflake, maintaining data quality and referential integrity.

### Data Model

**Star Schema Design:**
```
┌─────────────────┐
│  CUSTOMER_DIM   │────┐
│ (SCD Type 2)    │    │
└─────────────────┘    │
                       │
┌─────────────────┐    │      ┌──────────────────┐
│    CAR_DIM      │────┼──────│  RENTALS_FACT    │
└─────────────────┘    │      └──────────────────┘
                       │
┌─────────────────┐    │
│  LOCATION_DIM   │────┤
└─────────────────┘    │
                       │
┌─────────────────┐    │
│    DATE_DIM     │────┘
└─────────────────┘
```

### Verification Queries

**1. Check Customer Dimension (SCD Type 2):**
```sql
-- View current customer records
SELECT 
    customer_id,
    name,
    email,
    phone,
    effective_date,
    end_date,
    is_current
FROM car_rental.PUBLIC.customer_dim
WHERE is_current = TRUE
ORDER BY customer_id;

-- View customer history (all versions)
SELECT 
    customer_id,
    name,
    email,
    effective_date,
    end_date,
    is_current
FROM car_rental.PUBLIC.customer_dim
WHERE customer_id = 'C1001'
ORDER BY effective_date DESC;
```

**2. Check Rentals Fact Table:**
```sql
-- View recent rentals
SELECT 
    rental_id,
    customer_key,
    car_key,
    pickup_location_key,
    dropoff_location_key,
    start_date_key,
    end_date_key,
    rental_duration_days,
    total_rental_amount,
    average_daily_rental_amount,
    is_long_rental
FROM car_rental.PUBLIC.rentals_fact
ORDER BY start_date_key DESC
LIMIT 100;

-- Count records by date
SELECT 
    start_date_key,
    COUNT(*) as rental_count,
    SUM(total_rental_amount) as total_revenue
FROM car_rental.PUBLIC.rentals_fact
GROUP BY start_date_key
ORDER BY start_date_key DESC;
```

**3. Data Quality Checks:**
```sql
-- Check for orphaned records (missing dimension keys)
SELECT 
    COUNT(*) as orphaned_records
FROM car_rental.PUBLIC.rentals_fact
WHERE customer_key IS NULL
   OR car_key IS NULL
   OR pickup_location_key IS NULL
   OR dropoff_location_key IS NULL
   OR start_date_key IS NULL
   OR end_date_key IS NULL;

-- Verify referential integrity
SELECT 
    'customer' as dimension,
    COUNT(DISTINCT f.customer_key) as keys_in_fact,
    COUNT(DISTINCT d.customer_key) as keys_in_dimension
FROM car_rental.PUBLIC.rentals_fact f
LEFT JOIN car_rental.PUBLIC.customer_dim d ON f.customer_key = d.customer_key

UNION ALL

SELECT 
    'car',
    COUNT(DISTINCT f.car_key),
    COUNT(DISTINCT d.car_key)
FROM car_rental.PUBLIC.rentals_fact f
LEFT JOIN car_rental.PUBLIC.car_dim d ON f.car_key = d.car_key;
```

**4. Business Analytics Queries:**
```sql
-- Top 10 customers by revenue
SELECT 
    c.name,
    c.email,
    COUNT(f.rental_id) as total_rentals,
    SUM(f.total_rental_amount) as total_revenue
FROM car_rental.PUBLIC.rentals_fact f
JOIN car_rental.PUBLIC.customer_dim c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.name, c.email
ORDER BY total_revenue DESC
LIMIT 10;

-- Most popular car models
SELECT 
    car.make,
    car.model,
    car.year,
    COUNT(f.rental_id) as rental_count,
    AVG(f.rental_duration_days) as avg_rental_days
FROM car_rental.PUBLIC.rentals_fact f
JOIN car_rental.PUBLIC.car_dim car ON f.car_key = car.car_key
GROUP BY car.make, car.model, car.year
ORDER BY rental_count DESC
LIMIT 10;

-- Revenue by location
SELECT 
    l.location_name as pickup_location,
    COUNT(f.rental_id) as rental_count,
    SUM(f.total_rental_amount) as total_revenue,
    AVG(f.average_daily_rental_amount) as avg_daily_rate
FROM car_rental.PUBLIC.rentals_fact f
JOIN car_rental.PUBLIC.location_dim l ON f.pickup_location_key = l.location_key
GROUP BY l.location_name
ORDER BY total_revenue DESC;
```

### Expected Results

**Customer Dimension Sample:**
```
CUSTOMER_ID | NAME          | EMAIL              | EFFECTIVE_DATE       | END_DATE  | IS_CURRENT
------------|---------------|--------------------|--------------------- |-----------|------------
C1001       | John Smith    | john@email.com     | 2026-01-11 09:45:00 | NULL      | TRUE
C1002       | Jane Doe      | jane@email.com     | 2026-01-11 09:45:00 | NULL      | TRUE
C1003       | Bob Johnson   | bob@email.com      | 2026-01-11 09:45:00 | NULL      | TRUE
```

**Rentals Fact Sample:**
```
RENTAL_ID    | CUSTOMER_KEY | CAR_KEY | DURATION_DAYS | TOTAL_AMOUNT | IS_LONG_RENTAL
-------------|------------- |---------|---------------|--------------|---------------
R20260111001 | 1001         | 2001    | 4             | 359.96       | 0
R20260111002 | 1002         | 2002    | 7             | 559.93       | 0
R20260111003 | 1003         | 2003    | 10            | 899.90       | 1
```

---

## 🔧 Troubleshooting Common Issues

### Issue 1: Snowflake Connection Timeout

**Symptom:**
```
SnowflakeSQLException: JDBC driver encountered communication error
Connect to essbbdc-mi10939.snowflakecomputing.com:443 failed: connect timed out
```

**Solution:**
```bash
# Enable Cloud NAT for Dataproc cluster
gcloud compute routers create dataproc-nat-router \
    --network=default \
    --region=us-central1

gcloud compute routers nats create dataproc-nat-config \
    --router=dataproc-nat-router \
    --region=us-central1 \
    --nat-all-subnet-ip-ranges \
    --auto-allocate-nat-external-ips

# Verify connectivity
gcloud compute ssh hadoop-spark-cluster-m --zone=us-central1-c
curl -v https://essbbdc-mi10939.snowflakecomputing.com:443
```

### Issue 2: File Format Not Found

**Symptom:**
```
SQL compilation error: File format 'CSV_FORMAT' does not exist or not authorized
```

**Solution:**
```sql
-- Add schema context in SQL
USE DATABASE car_rental;
USE SCHEMA PUBLIC;

-- Or use fully qualified name
FILE_FORMAT => car_rental.PUBLIC.CSV_FORMAT
```

### Issue 3: XCom Pull Returns None

**Symptom:**
Task fails with `NoneType object has no attribute`

**Solution:**
```python
# Ensure upstream task pushes to XCom
def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date  # This is automatically pushed to XCom

# Check task dependencies
get_execution_date_task >> merge_customer_dim
```

---

## 🎓 Best Practices & Recommendations

### Data Quality
1. **Implement Data Validation**: Add data quality checks before loading
2. **Idempotency**: Ensure pipeline can be re-run safely
3. **Monitoring**: Set up alerts for failed tasks
4. **Logging**: Maintain detailed logs for troubleshooting

### Security
1. **Use Secret Manager**: Never hardcode credentials
2. **IAM Roles**: Use service accounts with minimal permissions
3. **Network Security**: Use VPC Service Controls and Private Service Connect
4. **Encryption**: Enable encryption at rest and in transit

### Performance
1. **Partition Data**: Use date-based partitioning in GCS
2. **Optimize Spark**: Tune executor memory and cores
3. **Snowflake Optimization**: Use clustering keys on fact table
4. **Incremental Loading**: Process only new/changed data

### Scalability
1. **Horizontal Scaling**: Increase Dataproc worker nodes
2. **Auto-scaling**: Enable Dataproc autoscaling
3. **Warehouse Sizing**: Scale Snowflake warehouse for peak loads
4. **Connection Pooling**: Implement connection pooling for Snowflake

---

## 📈 Monitoring & Observability

### Airflow Monitoring
- DAG run duration
- Task failure rates
- SLA misses
- Resource utilization

### Dataproc Monitoring
- Job execution time
- Shuffle spill
- GC time
- Memory pressure

### Snowflake Monitoring
```sql
-- Query performance
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE DATABASE_NAME = 'CAR_RENTAL'
ORDER BY START_TIME DESC
LIMIT 100;

-- Table storage
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES / (1024*1024*1024) as SIZE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'PUBLIC'
ORDER BY BYTES DESC;
```

---

## 🚀 Next Steps

1. **Automate Scheduling**: Change `schedule_interval` from `None` to `@daily`
2. **Add Data Quality Tests**: Implement Great Expectations or dbt tests
3. **Build Dashboards**: Connect Tableau/PowerBI to Snowflake
4. **Implement CI/CD**: Use GitHub Actions for DAG deployment
5. **Add Incremental Processing**: Optimize to process only changed data
6. **Setup Alerting**: Configure PagerDuty/Slack for fa
