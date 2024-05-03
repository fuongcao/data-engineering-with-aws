from datetime import datetime, timedelta
import pendulum
import os
import logging
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from final_project_helpers.table_create_statements import TableCreateqlQueries


@dag(
    start_date=pendulum.now()
)
def final_project_create_table_on_redshift():

    @task
    def create_final_project_tables():
        redshift_hook = PostgresHook("redshift")
        for query in TableCreateqlQueries.LIST_OF_QUERIES:
            logging.info(f"executing query \"{query}\" ...")
            redshift_hook.run(TableCreateqlQueries.LIST_OF_QUERIES[query])

    create_final_project_tables_task = create_final_project_tables()

final_project_create_table_on_redshift_dag = final_project_create_table_on_redshift()