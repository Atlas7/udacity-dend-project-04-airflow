from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Transform Redshift staging tables into Redshift Dimension Table
    Note: Dimension loads are often done with the truncate-insert pattern where the target
    table is emptied before the load. Thus, we could have a parameter that allows 
    switching between insert modes when loading dimensions. 
    """    
    
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql_to_load_tbl="",
                 reset_table=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table     
        self.sql_to_load_tbl = sql_to_load_tbl   
        self.reset_table = reset_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        if self.reset_table:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
            
        redshift.run(self.sql_to_load_tbl)
        self.log.info(f"LoadDimensionOperator completed for table: {self.table}")
        