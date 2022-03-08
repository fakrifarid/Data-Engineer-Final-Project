# STEP 1: Libraries needed
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
)
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.contrib.operators import bigquery_operator

from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG


#STEP 2:Define a start date
#In this case March 8th, 2022
yesterday = datetime(2022, 3, 8)

# Spark References
SPARK_CODE = ("gs://us-central1-final-project-886e3b53-bucket/spark/final_project_spark.py")
dataproc_job_name = "spark_job_dataproc"

# STEP 3: Set default arguments
default_dag_args = {
    "start_date": yesterday,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

# STEP 4: Define DAG
# Set DAG name, description, schedule interval, etc.
with models.DAG(
    "final_project",
    description="DAG for digital skola final project",
    schedule_interval=timedelta(days=1),
    default_args=default_dag_args
) as dag:

    # STEP 5: Set Operators
    # 5.1 - DummyOperator: start_pipeline
    start_pipeline = DummyOperator(
        task_id="start_pipeline",
    )

    # 5.2 - Dataproc Operator: creating small dataproc cluster
    create_dataproc = dataproc_operator.DataprocClusterCreateOperator(
        task_id="create_dataproc",
        project_id=models.Variable.get("project_id"),
        cluster_name="dataproc-cluster-final-project-{{ ds_nodash }}",
        num_workers=0,
        zone=models.Variable.get("dataproc_zone"),
        region=models.Variable.get("dataproc_region"),
        master_machine_type="n1-standard-4",
        worker_machine_type="n1-standard-4"
    )

    # 5.3 - Dataproc Running PySpark Job
    extract_to_parquet = dataproc_operator.DataProcPySparkOperator(
        task_id="extract_to_parquet",
        main=SPARK_CODE,
        cluster_name="dataproc-cluster-final-project-{{ ds_nodash }}",
        region=models.Variable.get("dataproc_region"),
        job_name=dataproc_job_name
    )

    # 5.4 - DummyOperator: load_to_staging_pipeline
    load_to_staging_pipeline = DummyOperator(
        task_id="load_to_staging_pipeline",
    )

    # 5.5a - Load PARQUET files in Google Cloud Storage to BigQuery
    load_country_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_country_to_bigquery",
        bucket="file_final_project",
        source_objects=["raw/uscountry.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.countries",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5b - Load PARQUET files in Google Cloud Storage to BigQuery
    load_port_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_port_to_bigquery",
        bucket="file_final_project",
        source_objects=["raw/usport.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.ports",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5c - Load PARQUET files in Google Cloud Storage to BigQuery
    load_state_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_state_to_bigquery",
        bucket="file_final_project",
        source_objects=["raw/usstate.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.states",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5d - Load PARQUET files in Google Cloud Storage to BigQuery
    load_weather_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_weather_to_bigquery",
        bucket="file_final_project",
        source_objects=["raw/globaltempbycity.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.weathers",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5e - Load PARQUET files in Google Cloud Storage to BigQuery
    load_demographic_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_demographic_to_bigquery",
        bucket="file_final_project",
        source_objects=["raw/demographic.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.demographic",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5f - Load PARQUET files in Google Cloud Storage to BigQuery
    load_airportcode_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_airportcode_to_bigquery",
        bucket="file_final_project",
        source_objects=["raw/airportcodes.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.airport_codes",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5g - Load PARQUET files in Google Cloud Storage to BigQuery
    load_immigration_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_immigration_to_bigquery",
        bucket="file_final_project",
        source_objects=["raw/immigration.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.immigration_data",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.6 - Dummy Operator before transforming staging to dimension
    transform_staging_1_to_dimension = DummyOperator(
        task_id = 'transform_staging_1_to_dimension'
    )

    transform_staging_2_to_dimension = DummyOperator(
        task_id = 'transform_staging_2_to_dimension'
    )

    # 5.7a - Load BigQuery from STAGING to DWH
    transform_country_to_dim = BigQueryOperator(
        task_id = "transform_country_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_COUNTRY.sql"
    )
    
    # 5.7b - Load BigQuery from STAGING to DWH
    transform_port_to_dim = BigQueryOperator(
        task_id = "transform_port_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_PORT.sql"
    )

    # 5.7c - Load BigQuery from STAGING to DWH
    transform_state_to_dim = BigQueryOperator(
        task_id = "transform_state_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_STATE.sql"
    )

    # 5.7d - Load BigQuery from STAGING to DWH
    transform_weather_to_dim = BigQueryOperator(
        task_id = "transform_weather_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_WEATHER.sql"
    )

    # 5.7e - Load BigQuery from STAGING to DWH
    transform_airport_to_dim = BigQueryOperator(
        task_id = "transform_airport_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_AIRPORT.sql"
    )

    # 5.7f - Load BigQuery from STAGING to DWH
    transform_city_demo_to_dim = BigQueryOperator(
        task_id = "transform_city_demo_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_CITY_DEMO.sql"
    )

    # 5.7g - Load BigQuery from STAGING to DWH
    transform_time_to_dim = BigQueryOperator(
        task_id = "transform_time_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_TIME.sql"
    )

    # 5.8 - Dummy Operator before transforming staging to dimension
    transform_staging_to_fact = DummyOperator(
        task_id = 'transform_staging_to_fact'
    )
    
    # 5.9 - Load BigQuery from STAGING to DWH
    transform_immigration_data = BigQueryOperator(
        task_id = "transform_immigration_data",
        use_legacy_sql = False,
        params = {
            "project_id": "agile-genius-342013",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/F_IMMIGRATION_DATA.sql"
    )

    # 5.10 - Dataproc Cluster deletion
    delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
        task_id="delete_dataproc",
        project_id=models.Variable.get("project_id"),
        region=models.Variable.get("dataproc_region"),
        cluster_name="dataproc-cluster-final-project-{{ ds_nodash }}",
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    finish_pipeline = DummyOperator(
        task_id="finish_pipeline",
    )

    # STEP 6: Set DAGs dependencies
    start_pipeline >> create_dataproc >> extract_to_parquet >> load_to_staging_pipeline >> [load_country_to_bigquery, load_port_to_bigquery, load_state_to_bigquery, load_weather_to_bigquery, load_demographic_to_bigquery, load_airportcode_to_bigquery, load_immigration_to_bigquery] >> transform_staging_1_to_dimension

    transform_staging_1_to_dimension >> [transform_country_to_dim, transform_port_to_dim, transform_state_to_dim] >> transform_staging_to_fact >> transform_immigration_data >> transform_staging_2_to_dimension >> [transform_weather_to_dim, transform_airport_to_dim, transform_city_demo_to_dim, transform_time_to_dim] >> delete_dataproc >> finish_pipeline
