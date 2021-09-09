from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import redshift_connect

class CreateTablesOperator(BaseOperator):

    ui_color = '#dd42f5'

    @apply_defaults
    def __init__(self,
                redshift_conn_id: str,
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        with redshift_connect(self.redshift_conn_id) as cursor:
            with open('create_tables.sql', 'r') as fp:
                cursor.execute(fp.read())

        return True