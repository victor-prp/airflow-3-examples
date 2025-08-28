"""
XCom Debugging Helper Functions

These functions help you debug and inspect XCom data during development.
You can import these into any DAG for debugging purposes.
"""

def debug_all_xcoms(**context):
    """
    Debug function to inspect all XCom data available to current task.
    Add this as a task in your DAG for debugging.
    """
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    print("🐛 === XCOM DEBUGGING SESSION ===")
    print(f"DAG: {dag_run.dag_id}")
    print(f"Run ID: {dag_run.run_id}")
    print(f"Execution Date: {context['logical_date']}")
    print()
    
    # Get all tasks that have run before this one
    completed_tasks = []
    for task_id in dag_run.get_task_instances():
        if task_id.state in ['success', 'skipped']:
            completed_tasks.append(task_id.task_id)
    
    print(f"🔍 Completed tasks to check: {completed_tasks}")
    print()
    
    # Check XComs from each completed task
    for task_id in completed_tasks:
        try:
            # Get default return value
            xcom_value = task_instance.xcom_pull(task_ids=task_id)
            if xcom_value is not None:
                print(f"📊 Task '{task_id}' XCom:")
                print(f"   Type: {type(xcom_value).__name__}")
                print(f"   Value: {str(xcom_value)[:200]}{'...' if len(str(xcom_value)) > 200 else ''}")
                print()
        except Exception as e:
            print(f"⚠️ Could not retrieve XCom from '{task_id}': {e}")
    
    return {"debug_completed": True, "tasks_checked": len(completed_tasks)}

def debug_specific_xcom(task_id, key='return_value', **context):
    """
    Debug a specific XCom value with detailed information.
    
    Usage in your DAG:
    PythonOperator(
        task_id='debug_extract_data',
        python_callable=lambda **ctx: debug_specific_xcom('extract_user_data', **ctx),
        dag=dag
    )
    """
    task_instance = context['task_instance']
    
    print(f"🔍 === DEBUGGING XCOM: {task_id} ===")
    
    try:
        xcom_value = task_instance.xcom_pull(task_ids=task_id, key=key)
        
        print(f"Task ID: {task_id}")
        print(f"XCom Key: {key}")
        print(f"Value Type: {type(xcom_value).__name__}")
        print(f"Value Length: {len(str(xcom_value)) if xcom_value else 'None'}")
        print()
        
        if xcom_value is None:
            print("❌ XCom value is None - possible causes:")
            print("   - Task hasn't run yet")
            print("   - Task failed")
            print("   - Function didn't return anything")
            print("   - Wrong task_id or key")
        else:
            print(f"✅ XCom Value:")
            if isinstance(xcom_value, (dict, list)):
                import json
                print(json.dumps(xcom_value, indent=2, default=str))
            else:
                print(f"   {xcom_value}")
        
        return {"value": xcom_value, "type": type(xcom_value).__name__}
        
    except Exception as e:
        print(f"❌ Error accessing XCom: {e}")
        return {"error": str(e)}

def debug_context_variables(**context):
    """
    Debug all available context variables.
    Useful for understanding what's available in **context.
    """
    print("🔍 === ALL CONTEXT VARIABLES ===")
    
    for key, value in context.items():
        try:
            value_type = type(value).__name__
            value_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
            print(f"{key:20} | {value_type:15} | {value_str}")
        except Exception as e:
            print(f"{key:20} | ERROR: {e}")
    
    print()
    print("🎯 Most Useful Context Variables:")
    print("   logical_date    - Execution time")
    print("   task_instance   - For XCom operations")
    print("   dag_run         - DAG run information")
    print("   task            - Current task object")
    print("   ds              - Date string")
    print("   run_id          - Unique run identifier")
    
    return {"context_keys": list(context.keys())}

# Quick debugging templates for common scenarios
def quick_debug_pipeline(**context):
    """
    Quick debug for data pipeline scenarios.
    Shows data flow through multiple tasks.
    """
    task_instance = context['task_instance']
    
    print("🔄 === PIPELINE DATA FLOW DEBUG ===")
    
    # Common pipeline task names
    pipeline_tasks = [
        'extract_user_data',
        'calculate_user_metrics', 
        'process_user_data',
        'enrich_data_with_context',
        'generate_report_summary'
    ]
    
    for i, task_id in enumerate(pipeline_tasks, 1):
        try:
            data = task_instance.xcom_pull(task_ids=task_id)
            if data:
                print(f"Step {i}: {task_id}")
                if isinstance(data, dict) and 'user_id' in data:
                    print(f"   User ID: {data.get('user_id', 'N/A')}")
                    print(f"   Keys: {list(data.keys())[:5]}...")
                else:
                    print(f"   Type: {type(data).__name__}")
                    print(f"   Value: {str(data)[:50]}...")
                print()
        except:
            print(f"Step {i}: {task_id} - No data available")
    
    return {"pipeline_steps_checked": len(pipeline_tasks)}
