# develop an Apache Airflow DAG that will:

# Extract data from a csv file
# Extract data from a tsv file
# Extract data from a fixed-width file
# Transform the data
# Load the transformed data into the staging area

# Open a terminal and create a directory structure for the staging area as follows:
# sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
# Execute the following commands to give appropriate permission to the directories.
# sudo chmod -R 777 /home/project/airflow/dags/finalassignment
# Download the data set from the source to the following destination using the curl command.
# sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz


from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago



dag_args = {
    "owner":"yourname",
    "start_date":days_ago(0),
    "email":["xyz@gmail.com"],
    "email_on_failure":True,
    "email_on_retry":True,
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    }

dag = DAG(
    "ETL_toll_data",
    schedule_interval = timedelta(day=1)
    default_args=dag_args,
    description="Apache airflow assignment",
)

download_dataset = BashOperator(
    task_id = "download_dataset",
    bash_command = "wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    dag = dag
)

untar_dataset = BashOperator(
    task_id = "untar_dataset",
    bash_command = "tar -xzf tolldata.tgz",
    dag = dag
)
    

unzip_data = BashOperator(
    task_id = "unzip_data",
    bash_command = 'tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging/',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id = "extract_from_csv".
    bash_command = "cut -d',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv"
    dag = dag 
)

extract_data_from_tsv = BashOperator(
    task_id = "extract_from_tsv".
    bash_command = "cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv| tr '\t' ','  > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv"
    dag = dag
)

extract_data_from_fixed_width = BashOperator(
    task_id = "extract_from_fixed_width",
    bash_command = "cat /home/project/airflow/dags/finalassignment/staging/payment-data.txt | tr -s ' ' | tr ' ' ',' | rev |cut -d',' -f1,2|rev > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv"
    dag = dag,
)

consolidate_data = BashOperator(
    task = "consolidate_data",
    bash_command = "paste -d',' \
        /home/project/airflow/dags/finalassignment/staging/csv_data.csv \
        /home/project/airflow/dags/finalassignment/staging/tsv_data.csv \
        /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv \
        > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv"
    dag = dag,
)

# transform_data = BashOperator(
#     task = "transform_data",
#     bash_command = ,
#     dag = dag
# )

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    INPUT="/home/project/airflow/dags/finalassignment/staging/extracted_data.csv"
    OUTPUT="/home/project/airflow/dags/finalassignment/staging/transformed_data.csv"

    > "$OUTPUT"  # empty the output file if it exists

    while IFS=',' read -r c1 c2 c3 c4 c5 c6 c7 c8 c9
    do
        c4_upper=$(echo "$c4" | tr '[:lower:]' '[:upper:]')
        echo "$c1,$c2,$c3,$c4_upper,$c5,$c6,$c7,$c8,$c9" >> "$OUTPUT"
    done < "$INPUT"
    """
)

download_dataset >> untar_dataset >> unzip_data >> extract_data_from_csv >> extract_data_from_fixed_width >> extract_data_from_tsv >> consolidate_data >> transform_data
