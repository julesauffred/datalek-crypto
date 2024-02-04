from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from extraction import extraction_from_api_and_save_to_hdfs

airflow_owner = ""  # Nom du propriétaire du projet (Ton nom)
dag_name = ""  # Nom du DAG (exemple: datalake_dag)
jar_path = ""  # Chemin du fichier java généré avec sbt clean package

ingestion_task_name = ""  # Nom de la tache d'ingestion (expemple: ingestion_task)

formatted_task_name = ""  # Nom de la tache de formattage (expemple: formatted_task)
java_class_of_formatted_task = ""   # Nom de la classe à exécuter pour le formattage (expemple: main.scala.mnm.MnMcount)

combine_task_name = ""  # Nom de la tache de combinaison (expemple: combined_task)
java_class_of_combine_task = ""  # Nom de la classe à exécuter pour la combinaison (expemple: main.scala.mnm.MnMcount)


def extraction_callable(**kwargs):
    extraction_from_api_and_save_to_hdfs(kwargs["ds"])


default_args = {
    "owner": airflow_owner,
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
        dag_id=dag_name,
        description="This is dag to manage the data lake",
        schedule_interval="@daily",
        start_date=datetime(2024, 0o1, 0o1),
        default_args=default_args
) as dag:
    pass
    extraction_task = PythonOperator(
        task_id=ingestion_task_name,  # extraction
        python_callable=extraction_callable,
        provide_context=True
    )

    formatted_task = SparkSubmitOperator(
        task_id=formatted_task_name,  # formatted
        conn_id='spark_default',
        application=jar_path,
        java_class=java_class_of_formatted_task,
        conf={
            'spark.airflow.execution_date': '{{ ds }}'
        },
    )

    combined_task = SparkSubmitOperator(
        task_id=combine_task_name,  # combined
        conn_id='spark_default',
        application=jar_path,
        java_class=java_class_of_combine_task,
        conf={
            'spark.airflow.execution_date': '{{ ds }}'
        },
    )

    extraction_task >> formatted_task >> combined_task
