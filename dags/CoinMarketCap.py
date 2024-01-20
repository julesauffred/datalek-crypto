from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import json
from requests import Session
from requests.exceptions import ConnectionError, Timeout
from hdfs import InsecureClient  # Assurez-vous d'avoir installé le package hdfs

# Default arguments for the DAG
default_args = {
    'owner': 'jules',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creating a DAG instance
dag = DAG(
    'big_data_project_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Function to extract data from CoinMarketCap API
def extract_data(**kwargs):
    print("Step 1: Extracting data from CoinMarketCap API...")

    url = 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
        'start': '1',
        'limit': '5000',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': '532c9f49-8b75-4e45-96d2-5fdd0f16d3d8',
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        print("Step 2: API response received successfully.")
        return data

    except (ConnectionError, Timeout) as e:
        print(f"Error during API request: {e}")

# Function to store data on Google Cloud Storage
def store_on_gcs(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')

    current_timestamp = datetime.now().strftime("%H-%M-%S")
    folder_date_format = datetime.now().strftime("%d-%m-%Y")
    filename = f'crypto_data_{current_timestamp}.json'

    gcs_bucket = 'jules-bucket-storage'
    gcs_object_path = f'big_data_projet/raw/coinmarketcap/{folder_date_format}/CryptoList_{current_timestamp}.json'

    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_object_path)
    blob.upload_from_string(json.dumps(data), content_type='application/json')

    print(f"Step 3: Data has been stored on Google Cloud Storage in gs://{gcs_bucket}/{gcs_object_path}")

    return gcs_object_path

# Function to store data on HDFS
def store_on_hdfs(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')

    current_timestamp = datetime.now().strftime("%H-%M-%S")
    hdfs_path = f'/cryptodataset/raw/CryptoList_{current_timestamp}.json'

    client = InsecureClient('http://localhost:9870', user='hadoop')  # Update with your HDFS configuration
    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(json.dumps(data))

    print(f"Step 4: Data has been stored on HDFS at {hdfs_path}")

# Create tasks using PythonOperator
task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

task_store_on_gcs = PythonOperator(
    task_id='store_on_gcs',
    python_callable=store_on_gcs,
    provide_context=True,
    dag=dag,
)

task_store_on_hdfs = PythonOperator(
    task_id='store_on_hdfs',
    python_callable=store_on_hdfs,
    provide_context=True,
    dag=dag,
)

# Define the order of task execution in the DAG
task_extract_data >> task_store_on_gcs >> task_store_on_hdfs
