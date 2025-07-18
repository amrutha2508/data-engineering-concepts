from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago


input_file = "/etc/passwd"
extracted_file = 'extracted-data.txt'
transformed_file = 'transformed.txt'
output_file = 'data_for_analytics.csv'

def extract():
    global input_file
    print('Inside extract')
    with open(input_file,'r') as infile, open(extracted_file,'w') as outfile:
        for line in infile:
            fields = line.split(':')
            if len(fields)>=6:
                field_1 = fields[0]
                field_3 = fields[2]
                field_6 = fields[5]
                outfile.write(field_1+":"+field_3+":"+field_6+"\n")

def transform():
    global extracted_file, transformed_file
    print("Insode Transform")
    with open(extracted_file,'r') as infile, open(transformed_file, 'w') as outfile:
        for line in infile:
            processed_line = line.replace(':',',')
            outfile.write(processed_line+"\n")

def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a CSV file
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
            for line in infile:
                outfile.write(line+'\n')

def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line) 


default_args = {
    'owner':'Your name',
    'start_date': days_ago(0),
    'email':['abc@gmail.com'],
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG(
    'my-first-python-etl-dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(minutes=1),
)

execute_extract = PythonOperator(task_id ="extract",python_callable=extract,dag=dag)
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)
execute_extract >> execute_transform >> execute_load >> execute_check
