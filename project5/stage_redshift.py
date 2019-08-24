from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_statements = {'json':"""
    COPY {} 
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}' 
    FORMAT json '{}'
    TIMEFORMAT AS 'epochmillisecs'
    """,
    'csv':"""
    COPY {} 
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}' 
    DELIMITER ','
    TIMEFORMAT AS 'epochmillisecs'
    """}

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table='',
                 s3_bucket='s3://udacity-dend',
                 data_format="json",
                 json_path='auto',
                 aws_credentials_id="aws_credentials",
                 redshift_conn_id='redshift',
                 region='us-west-2',
                 s3_key='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.data_format = data_format
        self.json_path = json_path
        self.aws_credentials = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.region = region
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.copy_sql = self.copy_statements[data_format]

    def execute(self, context):
        # get credentials
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clear staging table if it exists
        self.log.info("Deleting destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # copy data
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info('S3 bucket: '.format(s3_path))
        if self.data_format == 'json':
            formatted_sql = StageToRedshiftOperator.copy_statements[self.data_format].format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.json_logs
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_statements[self.data_format].format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region
            )
        redshift.run(formatted_sql)
        #self.log.info('StageToRedshiftOperator not implemented yet')
        # load 





