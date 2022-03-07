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
#In this case yesterday
yesterday = datetime(2022, 3, 7)


# Spark references
SPARK_CODE = ('gs://us-central1-final-project-886e3b53-bucket/spark/final_project_spark.py')
dataproc_job_name = 'final_project_job'

# STEP 3: Set default arguments for the DAG
default_dag_args = {
    'start_date': yesterday,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with models.DAG(
    'final_project',
    description='DAG for digital skola final project',
    schedule_interval=timedelta(days=1),
    default_args=default_dag_args
) as dag:

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline'
    )

    # dataproc_operator
    # Create small dataproc cluster
    create_dataproc =  dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc',
        project_id=models.Variable.get('project_id'),
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        num_workers=0,
        zone=models.Variable.get('dataproc_zone'),
        region=models.Variable.get('dataproc_region'),
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2'
    )

    # Run the PySpark job
    run_spark = dataproc_operator.DataProcPySparkOperator(
        task_id='run_spark',
        main=SPARK_CODE,
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        region=models.Variable.get('dataproc_region'),
        job_name=dataproc_job_name
    )

    load_to_staging_pipeline = DummyOperator(
            task_id = 'load_to_staging_pipeline'
    )

    #load staging temp to bigquery
    load_temp_by_city_to_bq= GoogleCloudStorageToBigQueryOperator(
        task_id="load-raw-globaltempbycity-to-bigquery",
        bucket="file_final_project",
        source_objects=["raw/globaltempbycity.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.temp_by_city",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"

    )

    #load staging airport to bigquery
    load_airport_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load-raw-airportcodes-to-bigquery",
        bucket="file_final_project",
        source_objects=["raw/airportcodes.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.airport",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"

    )

    #load imigration to bigquery
    load_imigration_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="load-raw-immigration-to-bigquery",
        bucket="file_final_project",
        source_objects=["raw/immigration.parquet/part*"],
        destination_project_dataset_table="agile-genius-342013:FINAL_PROJECT_STAGING.immigration",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"

    )

    # Transform, load IMIGRATION DATA AS FACT TABLE
    create_immigration_data = BigQueryOperator(
        task_id = 'create_immigration_data',
        use_legacy_sql = False,
        params = {
            'project_id': "agile-genius-342013",
            'staging_dataset': "FINAL_PROJECT_STAGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/F_IMMIGRATION_DATA.sql'
    )


    transform_staging_to_dimension = DummyOperator(
            task_id = 'transform_staging_to_dimension'
    )

    # Transform, load IMIGRATION DATA AS FACT TABLE
    transform_airport_to_dim = BigQueryOperator(
        task_id = 'transform_airport_to_dim',
        use_legacy_sql = False,
        params = {
            'project_id': "agile-genius-342013",
            'staging_dataset': "FINAL_PROJECT_STAGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_AIRPORT.sql'
    )

    # Transform, load IMIGRATION DATA AS FACT TABLE
    transform_city_to_dim = BigQueryOperator(
        task_id = 'transform_city_to_dim',
        use_legacy_sql = False,
        params = {
            'project_id': "agile-genius-342013",
            'staging_dataset': "FINAL_PROJECT_STAGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_CITY_DEMO.sql'
    )

    # dataproc_operator
    # Delete Cloud Dataproc cluster.
    delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc',
        project_id=models.Variable.get('project_id'),
        region=models.Variable.get('dataproc_region'),
        cluster_name='dataproc-cluster-demo-{{ ds_nodash }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    finish_pipeline = DummyOperator(
    task_id = 'finish_pipeline'
)

    # STEP 6: Set DAGs dependencies
    # Each task should run after have finished the task before.

    start_pipeline >> create_dataproc >> run_spark >> load_to_staging_pipeline >> [load_temp_by_city_to_bq,load_airport_to_bq,load_imigration_to_bq]
    
    [load_temp_by_city_to_bq,load_airport_to_bq,load_imigration_to_bq] >> create_immigration_data >> transform_staging_to_dimension

    transform_staging_to_dimension >> [transform_airport_to_dim,transform_city_to_dim] >> delete_dataproc >> finish_pipeline
