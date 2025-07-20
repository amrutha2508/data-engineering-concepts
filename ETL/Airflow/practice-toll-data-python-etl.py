from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import tarfile
import csv
import requests

dag_args = {
    "owner": "yourname",
    "start_date": days_ago(0),
    "email": ["xyz@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "ETL_toll_data_python",
    default_args=dag_args,
    description="Apache Airflow Assignment using PythonOperators",
    schedule_interval=timedelta(days=1),
)

staging_dir = "/home/project/airflow/dags/finalassignment/staging"
os.makedirs(staging_dir, exist_ok=True)

def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    response = requests.get(url)
    with open("tolldata.tgz", "wb") as f:
        f.write(response.content)

def untar_dataset():
    with tarfile.open("tolldata.tgz") as tar:
        tar.extractall()

def unzip_data():
    with tarfile.open("/home/project/airflow/dags/finalassignment/tolldata.tgz") as tar:
        tar.extractall(path=staging_dir)

def extract_data_from_csv():
    with open(f"{staging_dir}/vehicle-data.csv", newline='') as infile, \
         open(f"{staging_dir}/csv_data.csv", "w", newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            writer.writerow(row[:4])


def extract_data_from_tsv():
    with open(f"{staging_dir}/tollplaza-data.tsv") as infile, \
         open(f"{staging_dir}/tsv_data.csv", "w") as outfile:
        for line in infile:
            fields = line.strip().split('\t')[4:7]
            outfile.write(",".join(fields) + "\n")

def extract_data_from_fixed_width():
    with open(f"{staging_dir}/payment-data.txt") as infile, \
         open(f"{staging_dir}/fixed_width_data.csv", "w") as outfile:
        for line in infile:
            parts = " ".join(line.split()).split(" ")
            outfile.write(",".join(parts[-2:]) + "\n")

def consolidate_data():
    with open(f"{staging_dir}/csv_data.csv") as f1, \
         open(f"{staging_dir}/tsv_data.csv") as f2, \
         open(f"{staging_dir}/fixed_width_data.csv") as f3, \
         open(f"{staging_dir}/extracted_data.csv", "w") as out:
        for row1, row2, row3 in zip(f1, f2, f3):
            out.write(row1.strip() + "," + row2.strip() + "," + row3.strip() + "\n")

def transform_data():
    with open(f"{staging_dir}/extracted_data.csv") as infile, \
         open(f"{staging_dir}/transformed_data.csv", "w") as outfile:
        for line in infile:
            fields = line.strip().split(',')
            fields[3] = fields[3].upper()  # transform vehicle_type
            outfile.write(",".join(fields) + "\n")

# PythonOperator tasks
download_dataset_task = PythonOperator(task_id="download_dataset", python_callable=download_dataset, dag=dag)
untar_dataset_task = PythonOperator(task_id="untar_dataset", python_callable=untar_dataset, dag=dag)
unzip_data_task = PythonOperator(task_id="unzip_data", python_callable=unzip_data, dag=dag)
extract_csv_task = PythonOperator(task_id="extract_data_from_csv", python_callable=extract_data_from_csv, dag=dag)
extract_tsv_task = PythonOperator(task_id="extract_data_from_tsv", python_callable=extract_data_from_tsv, dag=dag)
extract_fixed_task = PythonOperator(task_id="extract_data_from_fixed_width", python_callable=extract_data_from_fixed_width, dag=dag)
consolidate_task = PythonOperator(task_id="consolidate_data", python_callable=consolidate_data, dag=dag)
transform_task = PythonOperator(task_id="transform_data", python_callable=transform_data, dag=dag)


# DAG dependencies
download_dataset_task >> untar_dataset_task >> unzip_data_task >> extract_csv_task >> extract_fixed_task >> extract_tsv_task >> consolidate_task >> transform_task
