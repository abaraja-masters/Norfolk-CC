from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2023, 10, 16, 00, 00, 00),
    'retries': 1,
}

dag = DAG('bash_operator_dag', default_args=default_args, schedule_interval='@hourly')

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='bash /path/to/script.sh',
    dag=dag
)

dag.add_task(bash_task)
