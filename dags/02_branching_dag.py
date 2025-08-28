"""
Branching & Conditional Logic DAG - Airflow Training Step 2

This DAG demonstrates:
- BranchPythonOperator for conditional execution
- Multiple execution paths based on conditions
- EmptyOperator for flow control
- Task skipping and branch joining
- Dynamic decision making in workflows

Key Learning Objectives:
- How to create conditional workflows
- Understanding task branching and skipping
- Using context to make decisions
- Handling different execution paths
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import random

# Task ID constants to avoid typos
WEEKDAY_TASK = 'weekday_task'
WEEKEND_TASK = 'weekend_task'
RED_PATH = 'red_path'
YELLOW_PATH = 'yellow_path'
GREEN_PATH = 'green_path'
HIGH_VALUE_TASK = 'high_value_processing'
LOW_VALUE_TASK = 'low_value_processing'

# Default arguments
default_args = {
    'owner': 'airflow-training',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def check_day_of_week(**context):
    """
    Branch function that decides the execution path based on day of week.
    This demonstrates how to use context to make dynamic decisions.
    """
    # In Airflow 3.0+, use 'logical_date' instead of 'execution_date'
    logical_date = context['logical_date']
    day_of_week = logical_date.weekday()  # 0=Monday, 6=Sunday
    
    print(f"🗓️ Logical date: {logical_date}")
    print(f"📅 Day of week: {day_of_week} (0=Monday, 6=Sunday)")
    
    if day_of_week < 5:  # Monday to Friday (0-4)
        print("✅ It's a weekday - taking weekday path")
        return WEEKDAY_TASK
    else:  # Saturday or Sunday (5-6)
        print("🏖️ It's a weekend - taking weekend path")
        return WEEKEND_TASK

def check_random_condition():
    """
    Branch function that makes a random decision.
    This demonstrates non-deterministic branching.
    """
    random_number = random.randint(1, 100)
    print(f"🎲 Random number generated: {random_number}")
    
    if random_number <= 30:
        print("🔴 Red path chosen (<=30)")
        return RED_PATH
    elif random_number <= 70:
        print("🟡 Yellow path chosen (31-70)")
        return YELLOW_PATH
    else:
        print("🟢 Green path chosen (>70)")
        return GREEN_PATH

def check_data_condition(**context):
    """
    Branch function that uses XCom data from previous task.
    This demonstrates data-driven branching.
    """
    # Get data from previous task via XCom
    task_instance = context['task_instance']
    data_value = task_instance.xcom_pull(task_ids='generate_data')
    
    print(f"📊 Data received from previous task: {data_value}")
    
    if data_value > 50:
        print("📈 High value detected - taking high_value path")
        return HIGH_VALUE_TASK
    else:
        print("📉 Low value detected - taking low_value path")
        return LOW_VALUE_TASK

def generate_test_data():
    """
    Generate test data for the data-driven branch.
    """
    test_value = random.randint(1, 100)
    print(f"🔢 Generated test data: {test_value}")
    return test_value

def weekday_task():
    """Task executed on weekdays."""
    print("💼 Executing weekday business logic")
    print("⚙️ Processing regular business operations...")
    return "Weekday processing completed"

def weekend_task():
    """Task executed on weekends."""
    print("🏖️ Executing weekend maintenance logic")
    print("🔧 Running system maintenance and cleanup...")
    return "Weekend maintenance completed"

def red_path_task():
    """Task for red path."""
    print("🔴 Executing RED path logic")
    print("⚠️ Handling critical priority tasks...")
    return "Red path completed"

def yellow_path_task():
    """Task for yellow path."""
    print("🟡 Executing YELLOW path logic")
    print("⚡ Handling medium priority tasks...")
    return "Yellow path completed"

def green_path_task():
    """Task for green path."""
    print("🟢 Executing GREEN path logic")
    print("✅ Handling normal priority tasks...")
    return "Green path completed"

def high_value_processing():
    """Process high-value data."""
    print("💰 Processing high-value data")
    print("🔐 Applying enhanced security and validation...")
    return "High-value processing completed"

def low_value_processing():
    """Process low-value data."""
    print("📋 Processing standard data")
    print("⚡ Applying standard processing pipeline...")
    return "Standard processing completed"

def final_summary(**context):
    """
    Final task that demonstrates how to handle multiple possible upstream tasks.
    """
    print("📋 === EXECUTION SUMMARY ===")
    print("Gathering results from executed paths...")
    
    # This task will run regardless of which path was taken
    task_instance = context['task_instance']
    
    # Try to get results from different possible upstream tasks
    possible_tasks = [
        'weekday_task', 'weekend_task',
        'red_path', 'yellow_path', 'green_path',
        'high_value_processing', 'low_value_processing'
    ]
    
    executed_tasks = []
    for task_id in possible_tasks:
        try:
            result = task_instance.xcom_pull(task_ids=task_id)
            if result:
                executed_tasks.append(f"{task_id}: {result}")
        except:
            pass  # Task wasn't executed
    
    print("✅ Executed paths:")
    for task_result in executed_tasks:
        print(f"  - {task_result}")
    
    return f"Summary complete. {len(executed_tasks)} paths executed."

# Create the DAG
dag = DAG(
    'branching_conditional_logic_dag',
    default_args=default_args,
    description='DAG demonstrating branching and conditional logic patterns',
    schedule=timedelta(hours=2),  # Run every 2 hours for more variety
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['training', 'branching', 'conditional'],
)

# === WORKFLOW 1: Day-based Branching ===
start_task = EmptyOperator(
    task_id='start_workflow',
    dag=dag,
)

day_branch = BranchPythonOperator(
    task_id='check_day_of_week',
    python_callable=check_day_of_week,
    dag=dag,
)

weekday_task_op = PythonOperator(
    task_id=WEEKDAY_TASK,
    python_callable=weekday_task,
    dag=dag,
)

weekend_task_op = PythonOperator(
    task_id=WEEKEND_TASK,
    python_callable=weekend_task,
    dag=dag,
)

# === WORKFLOW 2: Random Branching ===
random_branch = BranchPythonOperator(
    task_id='random_decision',
    python_callable=check_random_condition,
    dag=dag,
)

red_path = PythonOperator(
    task_id=RED_PATH,
    python_callable=red_path_task,
    dag=dag,
)

yellow_path = PythonOperator(
    task_id=YELLOW_PATH,
    python_callable=yellow_path_task,
    dag=dag,
)

green_path = PythonOperator(
    task_id=GREEN_PATH,
    python_callable=green_path_task,
    dag=dag,
)

# === WORKFLOW 3: Data-driven Branching ===
generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_test_data,
    dag=dag,
)

data_branch = BranchPythonOperator(
    task_id='data_driven_branch',
    python_callable=check_data_condition,
    dag=dag,
)

high_value_task = PythonOperator(
    task_id=HIGH_VALUE_TASK,
    python_callable=high_value_processing,
    dag=dag,
)

low_value_task = PythonOperator(
    task_id=LOW_VALUE_TASK,
    python_callable=low_value_processing,
    dag=dag,
)

# === WORKFLOW CONVERGENCE ===
# These empty operators help join the branches back together
join_day_branch = EmptyOperator(
    task_id='join_day_branch',
    trigger_rule='none_failed_min_one_success',  # Run if at least one upstream succeeded
    dag=dag,
)

join_random_branch = EmptyOperator(
    task_id='join_random_branch',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

join_data_branch = EmptyOperator(
    task_id='join_data_branch',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

final_task = PythonOperator(
    task_id='final_summary',
    python_callable=final_summary,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Define the workflow dependencies
# Day-based branching
start_task >> day_branch
day_branch >> [weekday_task_op, weekend_task_op]
[weekday_task_op, weekend_task_op] >> join_day_branch

# Random branching  
start_task >> random_branch
random_branch >> [red_path, yellow_path, green_path]
[red_path, yellow_path, green_path] >> join_random_branch

# Data-driven branching
start_task >> generate_data >> data_branch
data_branch >> [high_value_task, low_value_task]
[high_value_task, low_value_task] >> join_data_branch

# Final convergence
[join_day_branch, join_random_branch, join_data_branch] >> final_task
