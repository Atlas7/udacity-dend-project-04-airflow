from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
  
        # basic test: there must be at least 1 record in the table
        records=redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")                
        if len(records) < 1 or len(records[0]) < 1:
            #https://knowledge.udacity.com/questions/883543
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")

        # additional tests as defined by the `dq_checks` parameter            
        if self.dq_checks:
            
            for dq_i, dq_check in enumerate(self.dq_checks):

                # get actual value
                test_sql = dq_check["test_sql"]
                actual_value = redshift.get_records(test_sql)[0][0]

                # prepare expected value
 
                comparison = dq_check["comparison"]
                expected_value = dq_check["expected_value"]
                dq_message = dq_check["test_description"]

                self.log.info(f"dq_message: {dq_message}")            
                self.log.info(f"test_sql: {test_sql}")
                self.log.info(f"comparison: {comparison}")
                self.log.info(f"actual_value: {test_sql}")
                self.log.info(f"expected_value: {test_sql}")

                # do DQ check
                if not comparison(actual_value, expected_value):
                    raise ValueError(f"Data quality check #{dq_i} failed. {dq_message}")
                else:
                    self.log.info(f"Data quality check #{dq_i} passed. {dq_message}")
                
                
        self.log.info("Data quality check completed")
        


        