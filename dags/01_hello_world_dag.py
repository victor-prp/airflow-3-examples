"""
Simple Hello World DAG - Airflow Basics Example

This DAG demonstrates the fundamental concepts of Apache Airflow:
- DAG definition and configuration
- Basic task creation using PythonOperator
- Task scheduling
- Simple logging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'learning-airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def hello_world_python():
    """
    A simple Python function that demonstrates basic logging in Airflow.
    This function will be executed as a task.
    """
    print("Hello World from Airflow!")
    print("This is a Python task running inside Airflow.")
    print(f"Current execution time: {datetime.now()}")
    return "Hello World task completed successfully!"


def greet_user(name="Airflow Learner"):
    """
    A Python function that takes parameters and demonstrates
    how to work with task context.
    """
    print(f"Hello, {name}!")
    print("Welcome to Apache Airflow!")
    
    # You can return values that can be used by downstream tasks
    return f"Greeting sent to {name}"


# Create the DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG for learning Airflow basics',
    schedule=timedelta(hours=1),  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't run for past dates
    tags=['tutorial', 'basics'],
)

# Task 1: Simple Python task
hello_task = PythonOperator(
    task_id='say_hello',
    python_callable=hello_world_python,
    dag=dag,
)

# Task 2: Python task with parameters
greet_task = PythonOperator(
    task_id='greet_user',
    python_callable=greet_user,
    op_kwargs={'name': 'Future Airflow Expert'},
    dag=dag,
)

# Task 3: Simple Bash task
bash_task = BashOperator(
    task_id='bash_hello',
    bash_command='echo "Hello from Bash! Date: $(date)"',
    dag=dag,
)

# Task 4: Another Python task to demonstrate task flow
def summarize_execution():
    """
    Demonstrates how to access execution context in Airflow.
    """
    print("=== Execution Summary ===")
    print("All previous tasks have completed successfully!")
    print("This demonstrates basic task orchestration in Airflow.")
    return "DAG execution completed!"

summary_task = PythonOperator(
    task_id='execution_summary',
    python_callable=summarize_execution,
    dag=dag,
)

# Define task dependencies
# This creates a simple linear flow: hello -> greet -> bash -> summary
hello_task >> greet_task >> bash_task >> summary_task

# Alternative syntax for defining dependencies:
# hello_task.set_downstream(greet_task)
# greet_task.set_downstream(bash_task)
# bash_task.set_downstream(summary_task)
