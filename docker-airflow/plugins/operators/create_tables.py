from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime

class CreateTableOperator(BaseOperator):
    """This class initiate the function and sources all necessary arguments"""
    @apply_defaults
    def __init__(self,
                 sql,
                 postgres_conn_id: str,
                 *args,
                 **kwargs):
        super().__init__(*args,**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
    
    def execute(self,context):
        """This function has the definition necessary for creating the tables"""
        start = datetime.now()
        postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        
        logging.info(f"Creating table")
        postgres.run(self.sql)
        logging.info(f"Created table")
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")