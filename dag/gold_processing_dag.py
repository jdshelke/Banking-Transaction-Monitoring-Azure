from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from datetime import datetime


PROJECT_HOME = "/home/jdshelke/projects/Banking-Transaction-Monitoring"
PROCESSING_JOB = f"{PROJECT_HOME}/src/jobs/gold_job.py"


GOLD_TABLES = [
    "customer_transaction_summary",
    "account_transaction_summary",
    "daily_transaction_metrics",
    "loan_payment_metrics",
    "card_fraud_summary",
    "branch_performance",
    "customer_risk_profile"
]


with DAG(
    dag_id="gold_processing",
    start_date=datetime(2026, 8, 15),
    schedule=None,
    catchup=False,
    tags=["banking", "processing", "gold"]
) as dag:

    gold_tasks = {}

    for table in GOLD_TABLES:

        gold_tasks[table] = BashOperator(
            task_id=f"process_{table}",
            cwd=PROJECT_HOME,
            pool="spark_pool",
            bash_command=(
                f"spark-submit "
                f"--master yarn "
                f"--deploy-mode client "
                f"--name Gold-Processing-{table} "
                f"{PROCESSING_JOB} "
                f"--table {table}"
            )
        )

    # ---------------------------------------------------------------
    # Gold -> Gold dependencies
    # ---------------------------------------------------------------

    # loan_payment_metrics is required by branch_performance
    gold_tasks["loan_payment_metrics"] >> \
        gold_tasks["branch_performance"]

    # customer_risk_profile requires:
    #   account_transaction_summary
    #   customer_transaction_summary
    #   loan_payment_metrics
    [
        gold_tasks["account_transaction_summary"],
        gold_tasks["customer_transaction_summary"],
        gold_tasks["loan_payment_metrics"]
    ] >> gold_tasks["customer_risk_profile"]