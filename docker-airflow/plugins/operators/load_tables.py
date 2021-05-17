from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime

class LoadTableOperator(BaseOperator):
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
        """This function has the definition necessary for loading data into tables"""
        postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        start = datetime.now()
        logging.info("Clearing data for each load")
        postgres.run("TRUNCATE TABLE {}".format(self.table))

        logging.info(f"Loading table {self.table}")
        sql =f"COPY {self.table} FROM STDIN DELIMITER ',' CSV HEADER"
        cursor.copy_expert(sql, open(self.path, "r"))
        conn.commit()
        logging.info(f"Loaded table {self.table}")
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")
