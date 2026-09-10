from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

PROJECT_HOME = "/home/jdshelke/projects/Banking-Transaction-Monitoring"
PROCESSING_JOB = f"{PROJECT_HOME}/src/jobs/silver_job.py"

SILVER_TABLES = [
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
    dag_id = "silver_processing",
    start_date = datetime(2026, 8, 15),
    schedule = None,
    catchup = False,
    tags = ["banking", "processing", "silver"]
) as dag:

    silver_tasks = []
    for table in SILVER_TABLES:

        task = BashOperator(
            task_id = f"process_{table}",
            cwd = PROJECT_HOME,
            pool = "spark_pool",
            bash_command = (
                f"spark-submit "
                f"--master yarn "
                f"--deploy-mode client "
                f"--name Silver-Processing-{table} "
                f"{PROCESSING_JOB} "
                f"--table {table}"
            )
        )
        silver_tasks.append(task)

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_processing",
        trigger_dag_id="gold_processing",
        wait_for_completion=False,
    )

    silver_tasks >> trigger_gold