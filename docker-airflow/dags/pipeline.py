#import all necessary modules
import logging
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.create_tables import CreateTableOperator
from operators.load_tables import LoadTableOperator 
from operators.data_quality import DataQualityOperator
from operators.data_analysis import DataAnalaysisOperator 

# from helpers import sql
from helpers import sql

# # Default settings applied to all tasks
default_args = {
    'owner': 'suryakolachana',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 17),
    'end_date': datetime(2021, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

#define the dag
with DAG('Fraud-Detection-Analysis',
        default_args=default_args,
        schedule_interval = '@monthly'
                    ) as dag: 

    #Start task definition
    t0 = DummyOperator(
        task_id='Start'
    )

    #Definition of tasks necessary for creating the tables
    t1 = CreateTableOperator(
        task_id = f"Create_Transactions",
        sql = sql.CREATE_TRANSACTIONS_SQL,
        postgres_conn_id = "postgres_conn"
    )

    t2 = CreateTableOperator(
        task_id = f"Create_Transaction_Identities",
        sql = sql.CREATE_TRANSACTION_IDENTITIES_SQL,
        postgres_conn_id = "postgres_conn"
    )

    #Definition of tasks necessary for copying the data into tables
    t3 = LoadTableOperator(
        task_id = f"Copy_Transactions",
        table ="TRANSACTIONS",
        path = "/usr/local/airflow/data/train_transaction.csv",
        postgres_conn_id = "postgres_conn"
    )

    t4 = LoadTableOperator(
        task_id = f"Copy_Transaction_Identities",
        table ="TRANSACTION_IDENTITIES",
        path = "/usr/local/airflow/data/train_identity.csv",
        postgres_conn_id = "postgres_conn",
    ) 

    #Definition of tasks necessary for data quality checks.
    t5 = DataQualityOperator(
    task_id = f"Transaction_rows_check",
    table ="TRANSACTIONS",
    path = "/usr/local/airflow/data/train_transaction.csv",
    postgres_conn_id = "postgres_conn"
    )
    
    t6 = DataQualityOperator(
    task_id = f"Transaction_Identity_rows_check",
    table ="TRANSACTION_IDENTITIES",
    path = "/usr/local/airflow/data/train_identity.csv",
    postgres_conn_id = "postgres_conn"
    )

    #Definition of tasks necessary for data analysis.
    t7 = DataAnalaysisOperator(
        task_id = f"Get_Fraud_Amounts_Metric",
        table = "FRAUD_AMOUNTS_METRIC",
        postgres_conn_id = "postgres_conn"
    )

    t8 = DataAnalaysisOperator(
    task_id = f"Get_Card_Product_detail_metric",
    table = "CARD_PRODUCT_DETAIL_METRIC",
    postgres_conn_id = "postgres_conn"
    )
    
    #End Task Definition.
    t9 = DummyOperator(
    task_id='End'
    )
    
    #Definition of the tasks flow
    t0 >> t1 >> t3 >> t4 >> t5 >> t7 >> t8 >> t9
    t0 >> t2 >> t3 >> t4 >> t6 >> t7 >> t8 >> t9
    

    