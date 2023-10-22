import sys
# Add the path '/opt/airflow' to the system path to import modules from that directory.
sys.path.append('/opt/airflow')
from download_rev_file import DownloadFiles
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.python import BranchPythonOperator 
from kafka import KafkaProducer
import pandas as pd
from airflow.operators.bash import BashOperator

# Task to fetch the revenue file from AWS S3
def fetch_revenue_file_from_aws():  
    obj = DownloadFiles()
    obj.fetch_revenue_file_from_aws()

# Task to get the week number
def get_week(ti):
    # Get the previous week number from the XCom
    week_number = ti.get_previous_ti().xcom_pull(task_ids='get_week_task', key='week_number')
    print("MY WEEK NUMBER", week_number)
    
    # If no previous week number exists, set the initial week number to 22
    if week_number is None:
        ti.xcom_push(key='week_number', value=22)
    
    # If the week number reaches 35, raise an exception to stop DAG execution
    elif week_number == 35:
        raise Exception("Week number is 35. Stopping DAG execution.")
    
    # If a previous week number exists, increment it by 1 and store it in XCom
    else:
        ti.xcom_push(key='week_number', value=week_number+1)

# Task to produce messages from the CSV file and send them to Kafka
def produce_message_from_csv(ti):
    # Path to the CSV file to be read
    csv_file_path = '/opt/airflow/csv_files/rev.csv'
    
    # Kafka bootstrap servers
    bootstrap_servers = 'kafka:9092'
    
    # Kafka topic name to send messages to
    topic_name = 'Weekly_data_topic'
    
    try:
        # Create a KafkaProducer instance with the specified bootstrap servers
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        
        # Retrieve the week number from the XCom variable
        week_number = ti.xcom_pull(task_ids='get_week_task', key="week_number")
        
        # If no week number is available, set it to 22 as the default value
        if week_number is None:
            week_number = 22
        else:
            week_number = int(week_number)

        print("Week number:", week_number)
        print("created")      
        
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)
        print("df created")
        
        # Filter the DataFrame based on the week number
        df_filtered = df[df['WEEK_NUMBER'] == week_number]

        # Iterate over the filtered DataFrame rows
        for _, row in df_filtered.iterrows():
            # Convert the row to JSON format
            message = row.to_json()
            message = message.encode('utf-8')
            
            # Send the message to the Kafka topic
            producer.send(topic_name, message)

        producer.close()
        
        # Increment the week number by 1 and store it in the XCom variable
        week_number += 1
        ti.xcom_push(key='week_number', value=str(week_number))

    except Exception as e:
        print(f'Failed to produce message from CSV: {e}')

  
# Task to determine if the next DAG run should be triggered or not
def next_dag_run(ti):
    week_number = ti.xcom_pull(task_ids='get_week_task', key="week_number")
    
    if int(week_number) < 36:
        return "trigger_next_dag_run_task" 
    else:
        return None

args = {
    'owner': 'airflow',
    'email': ['arin.a@sigmoidanalytics.com'],
    'email_on_failure': True,
    
}

# Define the DAG
with DAG(dag_id='telecom_project_dag', start_date=datetime(2022, 1, 1), schedule_interval=None, catchup=False, default_args=args) as dag:

    fetch_revenue_file_task = PythonOperator(
        task_id='fetch_revenue_file',
        python_callable=fetch_revenue_file_from_aws,
        dag=dag
    )

    get_week_task = PythonOperator(
        task_id='get_week_task',
        python_callable=get_week,
        dag=dag
    )

    producer_task = PythonOperator(
        task_id='producer_task',
        python_callable=produce_message_from_csv,
        dag=dag
    )

    should_run_next_dag = BranchPythonOperator(
        task_id='should_run_next_dag',
        python_callable=next_dag_run
    )
    
    trigger_next_dag_run_task = BashOperator(
        task_id='trigger_next_dag_run_task',
        bash_command='airflow dags trigger telecom_project_dag'
    )

    # Define the task dependencies
    fetch_revenue_file_task >> get_week_task >> producer_task >> should_run_next_dag  >> trigger_next_dag_run_task
