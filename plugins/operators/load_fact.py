from operators.load_dimension import LoadDimensionOperator
from airflow.utils.decorators import apply_defaults
from helpers import LoadModes


class LoadFactOperator(LoadDimensionOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 load_source,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(
            redshift_conn_id=redshift_conn_id,
            table=table,
            load_source=load_source,
            load_mode=LoadModes.append,
            *args, **kwargs)

        
