from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import AirflowException
from airflow.utils.decorators import apply_defaults

class DataQualityException(AirflowException):
    def __init__(self, result, expected):
        self.result = result
        self.expected = expected
        f"""{self.result} is not equal to {self.expected}"""

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table = '',
                 redshift_conn_id = 'redshift',
                 query = '',
                 db_name = '',
                 expected = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.db_name = db_name
        self.expected_result = expected

    def execute(self, context):
        self.log.info(self.table)
        self.log.info(self.db_name)
        self.log.info(self.query)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        result = redshift.run(self.query)
        
        if result != self.expected_result:
            self.log.error(f"{result} != {self.expected_result}")
            raise DataQualityException(result, self.expected_result)