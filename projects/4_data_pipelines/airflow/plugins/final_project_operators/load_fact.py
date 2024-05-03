from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    table_truncate = "TRUNCATE {}"

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 table='',
                 sql='',
                 delete_on_load=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.delete_on_load = delete_on_load

    def execute(self, context):
        self.log.info("LoadFactOperator is executing ...")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_on_load:
            self.log.info(f"{self.table_truncate.format(self.table)} ...")
            redshift_hook.run(self.table_truncate.format(self.table))

        self.log.info(f"Loading to fact table {self.table} ...")
        redshift_hook.run(f"INSERT INTO {self.table} {self.sql}")
