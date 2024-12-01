"""
A basic dbt DAG that shows how to run dbt commands via the BashOperator
Follows the standard dbt seed, run, and test pattern.
"""

from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator



DBT_PROJECT_DIR = "/opt/airflow/dbt"


with DAG(
    "BuildELT_dbt",
    start_date=datetime(2024, 11, 11),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval = None,
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": "{{ conn.snowflake_conn.login }}",
            "DBT_PASSWORD": "{{ conn.snowflake_conn.password }}",
            "DBT_ACCOUNT": "{{ conn.snowflake_conn.extra_dejson.account }}",
            "DBT_SCHEMA": "{{ conn.snowflake_conn.schema }}",
            "DBT_DATABASE": "{{ conn.snowflake_conn.extra_dejson.database }}",
            "DBT_ROLE": "{{ conn.snowflake_conn.extra_dejson.role }}",
            "DBT_WAREHOUSE": "{{ conn.snowflake_conn.extra_dejson.warehouse }}",
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    # print_env_var = BashOperator(
    #    task_id='print_aa_variable',
    #    bash_command='echo "The value of AA is: $DBT_ACCOUNT,$DBT_ROLE,$DBT_DATABASE,$DBT_WAREHOUSE,$DBT_USER,$DBT_TYPE,$DBT_SCHEMA"'
    # )

    dbt_run >> dbt_test >> dbt_snapshot
