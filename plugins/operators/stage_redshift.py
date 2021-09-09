from typing import Optional
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                redshift_conn_id: str,
                s3file: str,
                iam_role: Optional[str] = None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.redshift_hook.get_conn()
        cursor = conn.cursor()
        sql = self._get_create_table_sql(
            self._get_postgres_table_definition()
        )
        cursor.execute(sql)

        cursor.close()
        conn.commit()

        return True





