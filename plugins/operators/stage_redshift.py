from typing import Optional
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ('s3file',)

    ui_color = '#358140'
    copy_sql_template = """
    copy {table}
    from '{json_data}'
    region '{region}'
    iam_role '{iam_role}'
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
                iam_role: str,
                json_statement: str,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3file = s3file
        self.iam_role = iam_role
        self.region = region
        self.json_statement = json_statement

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # delete content of table
        self.log.info('drain table %s', self.table)
        redshift_hook.run(self.delete_sql_template.format(
            table=self.table))
    

        self.log.info('copy to table %s from path %s', self.table, self.s3file)
        redshift_hook.run(self.copy_sql_template.format(
            table=self.table,
            json_data=self.s3file,
            region=self.region,
            iam_role=self.iam_role,
            json_statement=self.json_statement
        ))





