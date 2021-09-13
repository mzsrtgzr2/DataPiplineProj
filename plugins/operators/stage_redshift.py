from typing import Optional
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ('s3file',)

    ui_color = '#358140'
    copy_sql_template = """
    copy {table}
    from '{json_data}'
    region '{region}'
    access_key_id '{access_key_id}'
    secret_access_key '{secret_access_key}'
    timeformat as 'epochmillisecs'
    json '{json_statement}'
    BLANKSASNULL 
    TRIMBLANKS
    TRUNCATECOLUMNS  
    COMPUPDATE OFF  
    """
    delete_sql_template = """
    TRUNCATE {table}
    """
    @apply_defaults
    def __init__(self,
                redshift_conn_id: str,
                table: str,
                s3file: str,
                region: str,
                aws_connection_id: str,
                json_statement: str,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3file = s3file
        self.aws_connection_id = aws_connection_id
        self.region = region
        self.json_statement = json_statement

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(aws_conn_id=self.aws_connection_id, client_type='s3')

        # delete content of table
        self.log.info('drain table %s', self.table)
        redshift_hook.run(self.delete_sql_template.format(
            table=self.table))

        self.log.info('copy to table %s from path %s', self.table, self.s3file)
        credentials = aws_hook.get_credentials()
        print(credentials)

        redshift_hook.run(self.copy_sql_template.format(
            table=self.table,
            json_data=self.s3file,
            region=self.region,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            json_statement=self.json_statement
        ))





