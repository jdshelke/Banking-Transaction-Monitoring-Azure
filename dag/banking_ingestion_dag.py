from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

PROJECT_HOME = "/home/jdshelke/projects/Banking-Transaction-Monitoring"
INGESTION_JOB = f"{PROJECT_HOME}/src/jobs/ingestion_job.py"

SOURCE_TABLES = [
    "branches",
    "employees",
    "customers",
    "accounts",
    "cards",
    "loans",
    "transactions",
    "card_transactions",
    "loan_payments",
    "support_tickets",
]

with DAG(
    dag_id="banking_ingestion",
    start_date=datetime(2026, 8, 15),
    schedule=None,
    catchup=False,
    tags=["banking", "ingestion", "bronze"]
) as dag:

    ingestion_tasks = []
    for table in SOURCE_TABLES:

        task = BashOperator(
            task_id=f"ingest_{table}",
            cwd=PROJECT_HOME,
            pool="spark_pool",
            bash_command=(
                f"spark-submit "
                f"--master yarn "
                f"--deploy-mode client "
                f"--name Banking-Ingestion-{table} "
                f"{INGESTION_JOB} "
                f"--table {table}"
            ),
        )
        ingestion_tasks.append(task)

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_processing",
        trigger_dag_id="silver_processing",
        wait_for_completion=False,
    )

    ingestion_tasks >> trigger_silver