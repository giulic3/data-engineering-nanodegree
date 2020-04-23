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
                 update_strategy="",  # append, overwrite
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        if update_strategy == "overwrite":
            # Update with truncate first strategy (only for dimension tables)
            query = 'TRUNCATE {}; INSERT INTO {} ({})'.format(self.table, self.table, self.sql)
        else if update_strategy == "append":
            query = 'INSERT INTO {} ({})'.format(self.table, self.sql)
        redshift.run(query)
        self.log.info('Success')
