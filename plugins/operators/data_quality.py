from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            if table=="songplays":
                check_nulls_query = """SELECT COUNT(*) FROM songplays 
                                              WHERE playid IS NULL OR 
                                              start_time IS NULL OR 
                                              userid IS NULL;
                                    """
                check_count_query = """SELECT COUNT(*) FROM songplays;"""
            
            if table=="users":
                check_nulls_query = """SELECT COUNT(*) FROM users 
                                              WHERE userid IS NULL;
                                    """
                check_count_query = """SELECT COUNT(*) FROM users;"""
            
            if table=="songs":
                check_nulls_query = """SELECT COUNT(*) FROM songs 
                                              WHERE songid IS NULL;
                                    """
                check_count_query = """SELECT COUNT(*) FROM songs;"""
        
            if table=="artists":
                check_nulls_query = """SELECT COUNT(*) FROM artists 
                                              WHERE artistid IS NULL;
                                    """
                check_count_query = """SELECT COUNT(*) FROM artists;"""
            
            if table=="time":
                check_nulls_query = """SELECT COUNT(*) FROM time 
                                              WHERE start_time IS NULL;
                                    """
                check_count_query = """SELECT COUNT(*) FROM time;"""
        
            self.log.info(f"Start quality checks for {table}")
            null_records = redshift.get_records(check_nulls_query)
            self.log.info(f"RESULTS: {null_records}")
            if len(null_records) < 1 or len(null_records[0]) < 1:
                raise ValueError(f"Data quality check failed.")
            num_records = null_records[0][0]
            if num_records > 0:
                raise ValueError(f"Data quality check failed.")
            self.log.info(f"Null check on {table} passed !!!")
            
            count_records = redshift.get_records(check_count_query)
            self.log.info(f"RESULTS: {count_records}")
            self.log.info(f"RESULTS: {table} had {count_records[0][0]} records.")
        
        self.log.info('DataQualityOperator has been implemented yet')
        self.log.info('Data Quality Checks Passed')