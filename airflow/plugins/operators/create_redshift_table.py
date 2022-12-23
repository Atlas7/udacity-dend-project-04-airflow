from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


# The code is borrowed from Udacity Data Engineering nanodegree course exercises from Lesson 3 (Data Pipeline).
# from `plugin/operators/s3_to_redshift.py`
class CreateRedshiftTableOperator(BaseOperator):
    ui_color = '#358140'
     
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 table_create_sql="",
                 drop_existing=False,
                 *args, **kwargs):

        super(CreateRedshiftTableOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_create_sql = table_create_sql
        self.drop_existing = drop_existing
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.drop_existing:
            self.log.info(f"Dropping: {self.table}")
            redshift.run(SqlQueries.table_drop.format(self.table))
        
        self.log.info(f"Creating: {self.table}")
        redshift.run(self.table_create_sql.format(self.table))