"""Count medals and check result freshness."""
import random
import time
from datetime import datetime, timezone
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

CONNECTION = "medals_mysql_db"

def pick_medal():
    return random.choice(["Bronze", "Silver", "Gold"])

def pick_branch(ti):
    return f"calc_{ti.xcom_pull(task_ids='pick_medal')}"

def generate_delay(params):
    seconds = params["delay_seconds"]
    print(f"Delay: {seconds} seconds", flush=True)
    time.sleep(seconds)

with DAG(
    dag_id="medals_dag", start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule_interval=None, catchup=False, max_active_runs=1,
    default_args={"retries": 0},
    params={"delay_seconds": Param(5, type="integer", minimum=0, maximum=120)},
    tags=["medals"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table", conn_id=CONNECTION,
        sql="""CREATE TABLE IF NOT EXISTS medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY, medal_type VARCHAR(10) NOT NULL,
            `count` INT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);""",
    )
    pick = PythonOperator(task_id="pick_medal", python_callable=pick_medal)
    branch = BranchPythonOperator(task_id="pick_medal_task", python_callable=pick_branch)
    delay = PythonOperator(task_id="generate_delay", python_callable=generate_delay,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    sensor = SqlSensor(
        task_id="check_for_correctness", conn_id=CONNECTION,
        sql="""SELECT COUNT(*) FROM medal_counts
            WHERE id = (SELECT MAX(id) FROM medal_counts)
            AND created_at >= CURRENT_TIMESTAMP - INTERVAL 30 SECOND
            AND created_at <= CURRENT_TIMESTAMP;""",
        fail_on_empty=True, poke_interval=2, timeout=12, mode="poke",
    )
    create_table >> pick >> branch
    for medal in ["Bronze", "Silver", "Gold"]:
        calculate = SQLExecuteQueryOperator(
            task_id=f"calc_{medal}", conn_id=CONNECTION,
            sql=f"""INSERT INTO medal_counts (medal_type, `count`, created_at)
                SELECT '{medal}', COUNT(*), CURRENT_TIMESTAMP
                FROM athlete_event_results WHERE medal = '{medal}';""",
        )
        branch >> calculate >> delay
    delay >> sensor
