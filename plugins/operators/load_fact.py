from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql='',
                 target_table='',
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.target_table = target_table
        self.truncate = truncate
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f'Truncate table {self.target_table}')
            redshift.run(f'TRUNCATE {self.target_table}')

        self.log.info(f'Load fact table {self.target_table}')
        redshift.run(f'INSERT INTO {self.target_table} {self.sql}')
