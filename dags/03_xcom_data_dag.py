"""
Data Exchange & Communication DAG - Airflow Training Step 3

This DAG demonstrates:
- XCom (Cross-Communication) for data passing between tasks
- Task return values and automatic XCom storage
- Manual XCom push and pull operations
- Template variables and Jinja2 templating
- Task context and metadata access
- Different data types in XComs (strings, numbers, lists, dicts)

Key Learning Objectives:
- How to pass data between tasks
- Understanding XCom storage and retrieval
- Using templates for dynamic task configuration
- Accessing task context and execution metadata
- Best practices for inter-task communication
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import json

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

# === DATA GENERATION FUNCTIONS ===

def extract_user_data():
    """
    Simulate extracting user data from a source system.
    Return value automatically becomes an XCom.
    """
    user_data = {
        'user_id': 12345,
        'username': 'airflow_learner',
        'email': 'learner@airflow.com',
        'registration_date': '2024-01-15',
        'is_premium': True,
        'score': 87.5
    }
    
    print(f"📊 Extracted user data: {user_data}")
    print("✅ Data will be automatically stored in XCom")
    
    # Return value automatically gets stored in XCom
    return user_data

def calculate_user_metrics():
    """
    Generate some calculated metrics.
    Shows different data types in XComs.
    """
    metrics = {
        'total_sessions': 142,
        'avg_session_duration': 25.7,  # minutes
        'favorite_features': ['data_pipelines', 'scheduling', 'monitoring'],
        'engagement_score': 8.9,
        'last_activity': '2024-08-27'
    }
    
    print(f"📈 Calculated metrics: {metrics}")
    return metrics

def get_system_info(**context):
    """
    Generate system information and demonstrate context access.
    Shows how to access execution metadata.
    """
    logical_date = context['logical_date']
    dag_run = context['dag_run']
    task_instance = context['task_instance']
    
    system_info = {
        'execution_date': logical_date.isoformat(),
        'dag_id': dag_run.dag_id if dag_run else 'unknown',
        'run_id': dag_run.run_id if dag_run else 'unknown',
        'task_id': task_instance.task_id if task_instance else 'unknown',
        'try_number': task_instance.try_number if task_instance else 1,
        'system_load': 0.65,
        'available_memory': '8.2GB',
        'processing_time': logical_date.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    print(f"🖥️ System info at execution: {system_info}")
    print(f"📅 Logical date: {logical_date}")
    print(f"🔄 DAG run ID: {dag_run.run_id if dag_run else 'N/A'}")
    
    return system_info

# === DATA PROCESSING FUNCTIONS ===

def process_user_data(**context):
    """
    Process user data by pulling it from XCom.
    Demonstrates XCom pull operations.
    """
    task_instance = context['task_instance']
    
    # Pull data from the extract_user_data task
    user_data = task_instance.xcom_pull(task_ids='extract_user_data')
    metrics = task_instance.xcom_pull(task_ids='calculate_user_metrics')
    
    print(f"📨 Received user data: {user_data}")
    print(f"📊 Received metrics: {metrics}")
    
    # Process the data
    processed_data = {
        'user_id': user_data['user_id'],
        'username': user_data['username'],
        'account_type': 'premium' if user_data['is_premium'] else 'standard',
        'engagement_level': 'high' if metrics['engagement_score'] > 8.0 else 'medium',
        'total_sessions': metrics['total_sessions'],
        'avg_duration_hours': round(metrics['avg_session_duration'] / 60, 2),
        'user_score': user_data['score'],
        'processed_at': datetime.now().isoformat()
    }
    
    print(f"⚙️ Processed data: {processed_data}")
    return processed_data

def enrich_data_with_context(**context):
    """
    Enrich processed data with system information.
    Shows combining multiple XCom sources.
    """
    task_instance = context['task_instance']
    
    # Pull data from multiple upstream tasks
    processed_data = task_instance.xcom_pull(task_ids='process_user_data')
    system_info = task_instance.xcom_pull(task_ids='get_system_info')
    
    print(f"📥 Processing data: {processed_data}")
    print(f"🖥️ System context: {system_info}")
    
    # Enrich the data
    enriched_data = {
        **processed_data,  # Merge existing data
        'system_context': {
            'execution_date': system_info['execution_date'],
            'dag_run_id': system_info['run_id'],
            'system_load': system_info['system_load'],
            'available_memory': system_info['available_memory']
        },
        'data_quality_score': 9.2,
        'enrichment_timestamp': datetime.now().isoformat(),
        'pipeline_version': '1.0.0'
    }
    
    print(f"✨ Enriched data: {enriched_data}")
    return enriched_data

def generate_report_summary(**context):
    """
    Generate a final report summary.
    Demonstrates accessing all upstream data.
    """
    task_instance = context['task_instance']
    
    # Get the final enriched data
    enriched_data = task_instance.xcom_pull(task_ids='enrich_data_with_context')
    
    print("📋 === FINAL REPORT SUMMARY ===")
    print(f"User: {enriched_data['username']} (ID: {enriched_data['user_id']})")
    print(f"Account Type: {enriched_data['account_type'].upper()}")
    print(f"Engagement Level: {enriched_data['engagement_level'].upper()}")
    print(f"Total Sessions: {enriched_data['total_sessions']}")
    print(f"Avg Session Duration: {enriched_data['avg_duration_hours']} hours")
    print(f"User Score: {enriched_data['user_score']}")
    print(f"Data Quality: {enriched_data['data_quality_score']}/10")
    print(f"Processed at: {enriched_data['processed_at']}")
    print(f"System Load: {enriched_data['system_context']['system_load']}")
    
    # Return a summary
    summary = {
        'report_type': 'user_analysis',
        'user_id': enriched_data['user_id'],
        'status': 'completed',
        'total_data_points': 15,
        'processing_pipeline': 'extract -> calculate -> process -> enrich -> report',
        'completion_time': datetime.now().isoformat()
    }
    
    print(f"📊 Report summary: {summary}")
    return summary

# === MANUAL XCOM OPERATIONS ===

def manual_xcom_operations(**context):
    """
    Demonstrate manual XCom push and pull operations.
    Shows advanced XCom usage patterns.
    """
    task_instance = context['task_instance']
    
    print("🔧 === MANUAL XCOM OPERATIONS ===")
    
    # Manual XCom push - storing multiple values with custom keys
    task_instance.xcom_push(key='status', value='processing')
    task_instance.xcom_push(key='batch_id', value='BATCH_2024_08_27_001')
    task_instance.xcom_push(key='processing_config', value={
        'timeout': 300,
        'retry_count': 3,
        'parallel_workers': 4,
        'chunk_size': 1000
    })
    
    print("✅ Pushed multiple XCom values with custom keys")
    
    # Pull data from previous tasks with different methods
    
    # Method 1: Pull specific task's return value (default key)
    user_data = task_instance.xcom_pull(task_ids='extract_user_data')
    print(f"📥 Pulled user data (default key): {user_data['username']}")
    
    # Method 2: Pull specific key from specific task
    status = task_instance.xcom_pull(task_ids='manual_xcom_operations', key='status')
    print(f"📥 Pulled status (custom key): {status}")
    
    # Method 3: Pull multiple tasks at once
    multiple_data = task_instance.xcom_pull(task_ids=['extract_user_data', 'calculate_user_metrics'])
    print(f"📥 Pulled from multiple tasks: {len(multiple_data)} items")
    
    # Return processing results
    return {
        'manual_operations_completed': True,
        'xcom_demonstrations': ['push_custom_keys', 'pull_default', 'pull_custom', 'pull_multiple'],
        'processed_user': user_data['username'] if user_data else 'unknown'
    }

def xcom_cleanup_and_summary(**context):
    """
    Final task to demonstrate XCom access patterns and cleanup.
    """
    task_instance = context['task_instance']
    
    print("🧹 === XCOM CLEANUP AND SUMMARY ===")
    
    # Collect all XComs from the DAG run
    all_xcom_data = {}
    
    # List of tasks that produce XComs
    xcom_tasks = [
        'extract_user_data',
        'calculate_user_metrics', 
        'get_system_info',
        'process_user_data',
        'enrich_data_with_context',
        'generate_report_summary',
        'manual_xcom_operations'
    ]
    
    for task_id in xcom_tasks:
        try:
            data = task_instance.xcom_pull(task_ids=task_id)
            if data:
                all_xcom_data[task_id] = data
                print(f"✅ Retrieved XCom from {task_id}: {type(data).__name__}")
        except Exception as e:
            print(f"⚠️ Could not retrieve XCom from {task_id}: {e}")
    
    # Pull custom keys from manual operations
    try:
        batch_id = task_instance.xcom_pull(task_ids='manual_xcom_operations', key='batch_id')
        config = task_instance.xcom_pull(task_ids='manual_xcom_operations', key='processing_config')
        
        if batch_id:
            all_xcom_data['batch_id'] = batch_id
        if config:
            all_xcom_data['processing_config'] = config
            
    except Exception as e:
        print(f"⚠️ Could not retrieve custom XComs: {e}")
    
    print(f"📊 Total XCom entries collected: {len(all_xcom_data)}")
    
    # Create final summary
    final_summary = {
        'xcom_data_exchange_demo': 'completed',
        'total_xcoms_processed': len(all_xcom_data),
        'data_flow_stages': len(xcom_tasks),
        'completion_time': datetime.now().isoformat(),
        'data_types_demonstrated': ['dict', 'list', 'string', 'number', 'boolean'],
        'xcom_patterns_shown': [
            'automatic_return_storage',
            'manual_push_pull', 
            'custom_keys',
            'multiple_task_pull',
            'context_access'
        ]
    }
    
    print(f"🎯 Final summary: {final_summary}")
    return final_summary

# Create the DAG
dag = DAG(
    'xcom_data_exchange_dag',
    default_args=default_args,
    description='DAG demonstrating XCom data exchange and communication patterns',
    schedule=timedelta(hours=3),  # Run every 3 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['training', 'xcom', 'data-exchange', 'communication'],
)

# === TASK DEFINITIONS ===

# Data extraction and generation tasks
extract_task = PythonOperator(
    task_id='extract_user_data',
    python_callable=extract_user_data,
    dag=dag,
)

metrics_task = PythonOperator(
    task_id='calculate_user_metrics',
    python_callable=calculate_user_metrics,
    dag=dag,
)

system_info_task = PythonOperator(
    task_id='get_system_info',
    python_callable=get_system_info,
    dag=dag,
)

# Data processing tasks
process_task = PythonOperator(
    task_id='process_user_data',
    python_callable=process_user_data,
    dag=dag,
)

enrich_task = PythonOperator(
    task_id='enrich_data_with_context',
    python_callable=enrich_data_with_context,
    dag=dag,
)

# Reporting task
report_task = PythonOperator(
    task_id='generate_report_summary',
    python_callable=generate_report_summary,
    dag=dag,
)

# Advanced XCom operations
manual_xcom_task = PythonOperator(
    task_id='manual_xcom_operations',
    python_callable=manual_xcom_operations,
    dag=dag,
)

# Final cleanup and summary
cleanup_task = PythonOperator(
    task_id='xcom_cleanup_and_summary',
    python_callable=xcom_cleanup_and_summary,
    dag=dag,
)

# Templated Bash task to demonstrate XCom in templates
templated_bash_task = BashOperator(
    task_id='templated_bash_example',
    bash_command="""
        echo "=== XCOM TEMPLATE DEMONSTRATION ==="
        echo "Execution Date: {{ logical_date }}"
        echo "DAG ID: {{ dag.dag_id }}"
        echo "Task ID: {{ task.task_id }}"
        echo "Run ID: {{ run_id }}"
        echo "User Data Available: {{ ti.xcom_pull(task_ids='extract_user_data')['username'] if ti.xcom_pull(task_ids='extract_user_data') else 'N/A' }}"
        echo "Processing completed successfully!"
    """,
    dag=dag,
)

# === WORKFLOW DEPENDENCIES ===

# Parallel data generation
[extract_task, metrics_task, system_info_task]

# Sequential processing pipeline
extract_task >> process_task
metrics_task >> process_task
process_task >> enrich_task
system_info_task >> enrich_task
enrich_task >> report_task

# Advanced operations branch
extract_task >> manual_xcom_task
manual_xcom_task >> cleanup_task

# Templated task gets data after processing
process_task >> templated_bash_task
templated_bash_task >> cleanup_task

# Final convergence
report_task >> cleanup_task
