import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

default_args = {
    "start_date": datetime(2025, 7, 29),
    "owner": "airflow",
}

dag = DAG(
    "marts_dag",
    schedule_interval="@daily",
    default_args=default_args,
    render_template_as_native_obj=False,
    catchup=False,
)

base_event_path = "/user/cosmays/data/events"
geo_path = "/user/cosmays/data/geo.csv"

t1 = SparkSubmitOperator(
    task_id="calculate_users_mart",
    dag=dag,
    application="/lessons/users_mart.py", 
    conn_id="yarn_spark",
    application_args=[
        "{{ ds }}", "30", base_event_path, geo_path,
        "/user/cosmays/data/analytics/users_mart"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    num_executors=2,
    executor_memory="4g",
    executor_cores=2,
)

t2 = SparkSubmitOperator(
    task_id="calculate_zones_mart",
    dag=dag,
    application="/lessons//zones_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "{{ ds }}", "30", base_event_path, geo_path,
        "/user/cosmays/data/analytics/zones_mart"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    num_executors=2,
    executor_memory="4g",
    executor_cores=2,
)

t3 = SparkSubmitOperator(
    task_id="calculate_recommendations_mart",
    dag=dag,
    application="/lessons//recommend_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "{{ ds }}", "30", base_event_path, geo_path,
        "/user/cosmays/data/analytics/recommend_mart"
    ],
    conf={"spark.driver.maxResultSize": "20g"},
    num_executors=2,
    executor_memory="4g",
    executor_cores=2,
)

t1 >> t2 >> t3
