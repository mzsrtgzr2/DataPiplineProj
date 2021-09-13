from typing import Tuple
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import DataCheck

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 checks: Tuple[DataCheck],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for data_check in self.checks:
            query = data_check.check
            records = redshift_hook.get_records(query)
            if not records or not records[0]:
                raise ValueError(f"{query} failed returned no results")
            num_records = records[0][0]

            if not data_check.op(num_records, data_check.value):
                raise ValueError(f"{query} contained 0 rows")
            self.log.info(f"{query} check passed with {records[0][0]} records")