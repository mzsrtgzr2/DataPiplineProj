from typing import Tuple
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 tables: Tuple[str],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            records = redshift_hook.get_records(f'select count(*) FROM {table}')
            if not records or not records[0]:
                raise ValueError(f"{table} failed returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"{table} contained 0 rows")
            self.log.info(f"{table} check passed with {records[0][0]} records")