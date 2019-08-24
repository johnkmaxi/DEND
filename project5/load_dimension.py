from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table = '',
                 redshift_conn_id = 'redshift',
                 query = '',
                 db_name = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.db_name = db_name

    def execute(self, context):
        self.log.info(self.table)
        self.log.info(self.db_name)
        self.log.info(self.query)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clear staging table if it exists
        self.log.info("Deleting destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # loading new data
        redshift.run(self.query)
        #self.log.info('LoadDimensionOperator not implemented yet')
