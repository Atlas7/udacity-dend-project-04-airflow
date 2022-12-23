from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    """
    Transform Redshift staging tables into Redshift Fact Table
    Note: Fact tables are usually so massive that they should only allow append type functionality.
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql_to_load_tbl="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table    
        self.sql_to_load_tbl = sql_to_load_tbl       
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.sql_to_load_tbl)
        self.log.info(f"LoadFactOperator completed for table: {self.table}")
