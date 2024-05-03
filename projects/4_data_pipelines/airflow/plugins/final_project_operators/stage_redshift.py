from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from final_project_helpers.sql_statements import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    table_truncate = "TRUNCATE {}"

    all_copy_to_redshift = ("""
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
    """)

    partition_copy_all_to_redshift = ("""
        COPY {} FROM '{}/{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
    """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='us-east-1',
                 json_format='auto ignorecase',
                 partition_load=False,
                 delete_on_load=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_format = json_format
        self.partition_load = partition_load
        self.delete_on_load = delete_on_load


    def execute(self, context):
        self.log.info("StageToRedshiftOperator is executing ...")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        if self.delete_on_load:
            self.log.info(f"{self.table_truncate.format(self.table)} ...")
            redshift_hook.run(self.table_truncate.format(self.table))

        # create S3 uri
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info(f"S3 uri {s3_path}")

        if self.partition_load:
            stage_to_redshif_sql = StageToRedshiftOperator.partition_copy_all_to_redshift.format(
                self.table, s3_path, context['logical_date'].year, context['logical_date'].month,
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.json_format
            )

        else:
            stage_to_redshif_sql = StageToRedshiftOperator.all_copy_to_redshift.format(
                self.table, s3_path,
                credentials.access_key,
                credentials.secret_key, 
                self.region,
                self.json_format
            )

        # Run query
        self.log.info(f"Copying data from bucket {s3_path} to Redshift table {self.table}")
        redshift_hook.run(stage_to_redshif_sql)






