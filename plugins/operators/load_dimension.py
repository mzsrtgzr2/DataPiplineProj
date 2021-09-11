from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import LoadModes

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_query_template = """
    INSERT INTO {table} {load_source}
    """

    delete_sql_template = """
    TRUNCATE {table}
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id: str,
                table: str,
                load_source: str,
                load_mode: LoadModes,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_source = load_source
        self.load_mode = load_mode

    def execute(self, context):
        redshift_hook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id)
        
        if self.load_mode == LoadModes.overwrite:
            self.log.info('deleting data from table %s', self.table)
            redshift_hook.run(
                self.delete_sql_template.format(
                    table=self.table
            ))

        query = self.load_query_template.format(
            table = self.table,
            source=self.load_source
        )

        redshift_hook.run(query)
