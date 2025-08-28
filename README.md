# 🚀 Airflow 3 Training Examples

**Hands-on Apache Airflow 3 training through progressive DAG examples**

This repository provides a structured learning path for beginners to master Apache Airflow through practical, real-world examples. Each step builds upon the previous one, taking you from basic concepts to advanced production patterns.

## 🎯 Target Audience

- **Beginners to Apache Airflow** with basic Python knowledge
- Developers familiar with Docker Compose
- Anyone looking to learn workflow orchestration concepts

## ⚡ Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Basic Python knowledge
- Cursor installed for AI assistance

### Get Started in 3 Steps

1. **Start Airflow**
   ```bash
   docker compose up
   ```

2. **Access Airflow UI**
   - Open http://localhost:8080
   - Login: `admin` / `admin`

3. **Begin Training**
   - Navigate to DAGs view
   - Start with `01_hello_world_dag`
   - Follow the progression outlined below

## 📚 Training Steps

| Step | DAG File | Status | Key Concepts | Documentation |
|------|----------|---------|--------------|---------------|
| **1** | `01_hello_world_dag.py` | ✅ Ready | Basic DAG structure, PythonOperator, BashOperator, dependencies | [Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html) |
| **2** | `02_branching_dag.py` | ✅ Ready | BranchPythonOperator, conditional flows, task skipping | [Branching](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#branching) |
| **3** | `03_xcom_data_dag.py` | ✅ Ready | XCom data exchange, task templating, context variables | [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html) |
| **4** | `04_sensors_external_dag.py` | ✅ Ready | FileSensor, S3KeySensor, external system integration | [Sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html) |
| **5** | `05_dynamic_tasks_dag.py` | ⏳ **Need to Create** | Dynamic task generation, task mapping, parallel processing | [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) |
| **6** | `06_error_handling_dag.py` | ⏳ **Need to Create** | Retry strategies, failure callbacks, SLA monitoring | [Error Handling](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#task-retries) |
| **7** | `07_advanced_scheduling_dag.py` | ⏳ **Need to Create** | Custom timetables, asset-based triggering, advanced patterns | [Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetables.html) |

## 📁 Project Structure

```
airflow-3-examples/
├── dags/                    # Airflow DAG definitions
│   ├── 01_hello_world_dag.py     # ✅ Basic concepts
│   ├── 02_branching_dag.py       # ✅ Conditional logic
│   ├── 03_xcom_data_dag.py       # ✅ Data exchange
│   ├── 04_sensors_external_dag.py# ✅ External systems
│   ├── 05_dynamic_tasks_dag.py   # ⏳ To be created
│   ├── 06_error_handling_dag.py  # ⏳ To be created
│   └── 07_advanced_scheduling_dag.py # ⏳ To be created
├── config/                  # Airflow configuration
├── logs/                    # Execution logs
├── docker-compose.yaml      # Local development setup
├── debug_xcom_helper.py     # XCom debugging utilities
└── AIRFLOW_TRAINING_PLAN.md # Detailed learning objectives
```

### 🔧 Utility Files

- **`debug_xcom_helper.py`**: Import this into any DAG to debug XCom data exchange
- **`AIRFLOW_TRAINING_PLAN.md`**: Detailed breakdown of learning objectives per step

## 🚧 Creating Missing DAGs (Steps 5-7)

The advanced DAGs (steps 5-7) are intentionally missing to encourage hands-on learning. Use **Cursor AI** to create them based on the concepts outlined in `AIRFLOW_TRAINING_PLAN.md`.

### Cursor Prompts for Missing DAGs:

**For Step 5 (Dynamic Tasks):**
```
Create 05_dynamic_tasks_dag.py that demonstrates:
- Dynamic task creation with loops
- Task mapping and parallel processing  
- Variable number of tasks based on data
- Task groups for organization
```

**For Step 6 (Error Handling):**
```
Create 06_error_handling_dag.py that shows:
- Retry strategies and backoff policies
- Failure callbacks and notifications
- SLA monitoring and alerting
- Task dependencies with failure handling
```

**For Step 7 (Advanced Scheduling):**
```
Create 07_advanced_scheduling_dag.py featuring:
- Custom timetables and complex schedules
- Manual triggers and external APIs
- Asset-based triggering (Airflow 3.0 feature)
- Backfill strategies
```

## 🎓 Learning Path

### Recommended Approach:
1. **Sequential Learning**: Complete steps 1-4 in order
2. **Hands-On Practice**: Run each DAG and examine the execution
3. **Ask Cursor Questions**: Use AI to deepen understanding at each step
4. **Experimentation**: Modify parameters and observe the effects
5. **Create Advanced DAGs**: Use Cursor to build steps 5-7
6. **Deep Dive**: Follow documentation links for advanced concepts

### 💬 Essential Questions to Ask Cursor AI:

**Understanding Airflow Architecture:**
- "How are DAGs deployed to Airflow and when do they become available?"
- "Explain the main Airflow components: Scheduler, Executor, Webserver, and Worker"
- "How does Airflow discover and parse DAG files?"

**Step-by-Step Learning Questions:**
- **Step 1**: "How does task dependency work with the `>>` operator?"
- **Step 1**: "What happens when a PythonOperator function returns a value?"
- **Step 2**: "How does BranchPythonOperator decide which tasks to skip?"
- **Step 2**: "What's the difference between skipped and failed tasks?"
- **Step 3**: "How is XCom data stored and retrieved between tasks?"
- **Step 3**: "When should I use XCom vs. external storage for data passing?"
- **Step 4**: "How do sensors work internally - are they polling or event-driven?"
- **Step 4**: "What happens if a sensor times out?"

**Practical Operation Questions:**
- "How do I trigger a DAG manually vs. scheduled runs?"
- "What's the difference between task execution date and actual run time?"
- "How do I view task logs and troubleshoot failures?"
- "What do the different task colors mean in the Graph View?"
- "How do I clear task states and re-run specific tasks?"

### Tips for Success:
- 🔍 **Use the Graph View**: Visualize task dependencies in the Airflow UI
- 📝 **Check Task Logs**: Understanding execution details is crucial
- 🧪 **Experiment Freely**: Modify DAGs and see what happens
- 💬 **Ask Questions**: Use Cursor AI to understand complex concepts
- 🔄 **Iterate**: Make small changes and test frequently

## 🔍 Debugging & Troubleshooting

### Common Issues:
- **Port 8080 in use**: Change the port in `docker-compose.yaml`
- **DAG not appearing**: Check for Python syntax errors in logs
- **Task failures**: Examine task logs in the Airflow UI

### XCom Debugging:
Import and use the debug helper:
```python
from debug_xcom_helper import debug_all_xcoms

# Add as a task in your DAG
debug_task = PythonOperator(
    task_id='debug_xcoms',
    python_callable=debug_all_xcoms,
    dag=dag
)
```

## 📖 Next Steps After Training

After completing all 7 steps, you'll be ready to:
- 🏗️ Build production Airflow workflows
- 🔗 Integrate with external systems and APIs  
- 🛡️ Implement robust error handling and monitoring
- 📊 Design scalable data pipelines
- ⚙️ Configure complex scheduling patterns

### Additional Resources:
- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Best Practices Guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Production Deployment](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html)
- [Community Forum](https://github.com/apache/airflow/discussions)

## 🎉 Happy Learning!

Start your Airflow journey with Step 1 and progress at your own pace. Remember: the best way to learn Airflow is by building and experimenting with real workflows!

---

**Need help?** Use Cursor AI to ask questions about any DAG concepts, patterns, or troubleshooting. The AI can help explain complex workflows and suggest improvements to your code.
