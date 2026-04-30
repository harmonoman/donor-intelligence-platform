"""
Hello World DAG
Ticket 1.3 — Airflow Local Environment Setup

Purpose:
    Validate that Airflow is running correctly and DAGs are discoverable.
    This DAG contains no business logic — it exists to prove the
    orchestration layer works before real pipeline DAGs are written.
"""

from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule=None,           # manual trigger only
    catchup=False,
    tags=["setup", "validation"],
)
def hello_world():

    @task
    def task_one():
        print("Task 1 — Airflow is running.")
        print("DAG is discoverable and executable.")

    @task
    def task_two():
        print("Task 2 — Pipeline orchestration is ready.")
        print("Donor Intelligence Platform DAGs can now be built.")

    # Define dependency — task_one must complete before task_two runs
    task_one() >> task_two()


hello_world()
