from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 query: str,
                 expected: any,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.expected = expected

    def execute(self, context):
        self.log.info('test %s, expected %s', self.query, self.expected)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        actual = redshift_hook.run(self.query)
        self.log.info('actual result = ', actual)