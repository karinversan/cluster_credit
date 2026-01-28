from datetime import datetime

from airflow.decorators import dag, task

from src.ml.db import init_db
from src.ml.load_features import load_csv_to_postgres
from src.ml.train import train
from src.ml.predict import batch_score


@dag(
    dag_id="train_and_score",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
)
def train_and_score_dag():
    @task
    def init_db_task():
        init_db()

    @task
    def load_data_task():
        load_csv_to_postgres()

    @task
    def train_model_task() -> str:
        return train()

    @task
    def batch_score_task(model_run_id: str) -> int:
        return batch_score(run_id=model_run_id, truncate=True)

    init_task = init_db_task()
    load_task = load_data_task()
    run_id = train_model_task()
    init_task >> load_task >> run_id
    batch_score_task(run_id)


train_and_score_dag()
