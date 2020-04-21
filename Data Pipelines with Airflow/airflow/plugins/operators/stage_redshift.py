from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="", # Destination table
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 data_format="csv",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers
        self.data_format=data_format.lower()     # 'csv', 'json'

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials() # Get access key and secret access key
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Clear data from dest table before creating it
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        # Selecting format
        if self.data_format == 'csv':
            autoformat = "DELIMITER '{}'".format(self.delimiter)
        elif self.data_format == 'json':
            json_option = self.jsonpaths or 'auto'
            autoformat = "FORMAT AS JSON '{}'".format(json_option)
            
        self.log.info("Copying data from S3 to Redshift")
        # Set S3 path based on execution dates  
        rendered_key = self.s3_key.format(**context)
        self.log.info('Rendered key is ' + rendered_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        # result: s3://s3_bucket/year/month/day/s3_key
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            autoformat
        )
        redshift.run(formatted_sql) # Run copy from S3 to Redshift





