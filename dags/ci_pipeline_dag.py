from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl import extract_data, transform_data, connectDB, load_data_db, load_data_csv
from datetime import date

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'carbon_intensity_etl',
    default_args=default_args,
    description='ETL Pipeline for Carbon Intensity Data',
    schedule_interval='@daily',  # Run daily
    catchup=False,  # Disable catchup to avoid backfilling
)

# Define the ETL tasks

def extract(**kwargs):
    start = date(2024, 1, 1)
    BASE_URL = f"https://api.carbonintensity.org.uk/regional/intensity/{start_date}/pt24h"
    print("Commenced Data Extraction!")
    data = extract_data(URL=BASE_URL)
    kwargs['ti'].xcom_push(key='extracted_data', value=data)  # Push data to XCom
    print("Ended Data Extraction!")

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='extracted_data')  # Pull data from XCom
    print("Commencing Transformation!")
    transformed_data = transform_data(data=data)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)  # Push transformed data to XCom
    print("Transformation Complete!")

def load_to_db(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data')  # Pull transformed data from XCom
    print("Connecting to DB....")
    conn, curr = connectDB()
    print('Loading Data to Database...')
    load_data_db(data=transformed_data, conn=conn, cur=curr)
    print("Data Loaded to DB!")

def load_to_csv(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data')  # Pull transformed data from XCom
    print("Saving Data to CSV...")
    load_data_csv(transformed_data)
    print("Data Saved to CSV!")

# Define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_to_db_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    provide_context=True,
    dag=dag,
)

load_to_csv_task = PythonOperator(
    task_id='load_to_csv',
    python_callable=load_to_csv,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> [load_to_db_task, load_to_csv_task]