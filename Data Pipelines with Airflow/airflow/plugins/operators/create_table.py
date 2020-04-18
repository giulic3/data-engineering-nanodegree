from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    
    sql_file="/home/workspace/airflow/create_tables.sql"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Create Redshift connection...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Read file to get SQL statements
        self.log.info('Read SQL file...')
        fd = open(CreateTableOperator.sql_file, 'r')
        sql = fd.read()
        fd.close()

        sql_queries = sql.split(';')
        self.log.info('Creating tables...')
        # Run each query
        for query in sql_queries:
            if query.rstrip() != '':
                redshift.run(query) # Each run creates a table
