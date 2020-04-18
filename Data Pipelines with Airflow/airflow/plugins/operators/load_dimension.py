from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        # Truncate-insert strategy
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        self.log.info('Loading values into dimension table {}...').format(str(self.table))
        redshift.run(self.sql)
        self.log.info('Success')
