
import contextlib
from airflow.hooks.postgres_hook import PostgresHook

@contextlib.contextmanager
def redshift_connect(redshift_conn_id: str):
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    conn = redshift_hook.get_conn()
    if not conn:
        raise ConnectionError(
            f'unable to connect to redshift cluster {redshift_conn_id}')

    cursor = conn.cursor()
    if not cursor:
        raise ConnectionError(
            f'unable to get cursor to redshift cluster {redshift_conn_id}')
    

    yield cursor
    
    cursor.close()
    conn.commit()