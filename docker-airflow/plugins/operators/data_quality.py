from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime


class DataQualityOperator(BaseOperator):
    """This class initiate the function and sources all necessary arguments"""
    @apply_defaults
    def __init__(self,
                 table: str,
                 path : str,
                 postgres_conn_id: str,
                 *args,
                 **kwargs):
        super().__init__(*args,**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.path = path
    
    def execute(self,context):
        """This function has the definition necessary for doing data quality check"""
        postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)

        start = datetime.now()
        records = postgres.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")

        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")
