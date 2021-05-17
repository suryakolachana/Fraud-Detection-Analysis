from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime

class DataAnalaysisOperator(BaseOperator):
    """This class initiate the function and sources all necessary arguments"""
    fraud_amount_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    with t_b as
    (with t_a
    as
    (select distinct ti.device_type,
            ti.device_info,
            t.transaction_dt/86400 as day,
            round(cast(sum(t.transaction_amt) as numeric),2) as Desktop_Fraud_Amounts
    from transactions t,
        transaction_identities ti
    where t.transaction_id = ti.transaction_id
    and t.is_fraud = 1
    and ti.device_type = 'desktop'
    and ti.device_info is not null
    group by ti.device_type,ti.device_info, t.transaction_dt)
    select day,device_type,string_agg(distinct device_info, ' , ') as List_Of_Devices,sum(Desktop_Fraud_Amounts) as Fraud_Amounts from t_a
    group by day,device_type),
    t_c as
    (with t_a
    as
    (select distinct ti.device_type,
            ti.device_info,
            t.transaction_dt/86400 as day,
            round(cast(sum(t.transaction_amt) as numeric),2) as Mobile_Fraud_Amounts
    from transactions t,
        transaction_identities ti
    where t.transaction_id = ti.transaction_id
    and t.is_fraud = 1
    and ti.device_type = 'mobile'
    and ti.device_info is not null
    group by ti.device_type,ti.device_info, t.transaction_dt)
    select day,device_type,string_agg(distinct device_info, ' , ') as List_Of_Devices,sum(Mobile_Fraud_Amounts) as Fraud_Amounts from t_a
    group by day,device_type) 
    select t_b.day,
        t_b.device_type as Desktop,
        t_b.List_Of_Devices as Desktop_Devices,
        t_b.fraud_amounts as Desktop_Fraud_Amounts,
        t_c.device_type as Mobile,
        t_c.list_of_devices as Mobile_Devices,
        t_c.fraud_amounts as Mobile_Fraud_Amounts
    from t_b,t_c 
    where t_b.day = t_c.day
    order by t_b.device_type,t_b.day;
    """

    card_product_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    select t.card1,
       t.card2,
	   t.card3,
	   t.card4,
	   t.card5,
	   t.card6,
	   case when t.is_fraud = 0 Then 'Legitimate'
	        when t.is_fraud = 1 Then  'Fraud'
	   End Transaction_Type,
	   string_agg(distinct t.product_cd, ' ,') as Associated_products,
	   string_agg(distinct t.p_emaildomain, ' , ') as Associated_emails,
	   count(distinct t.transaction_id) as Associated_Transaction_Counts
        from transactions t 
        where is_fraud in (0,1)
        and (card4 is not null or card6 is not null) 
        and exists (select 1 from transaction_identities ti where ti.transaction_id = t.transaction_id)
        group by t.card1,t.card2,t.card3,t.card4,t.card5,t.card6,Transaction_Type
        order by transaction_type,Associated_Transaction_Counts desc,card1,card2,card3,card4,card5,card6;
    """

    @apply_defaults
    def __init__(self,
                 table,
                 postgres_conn_id: str,
                 *args,
                 **kwargs):
        super().__init__(*args,**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
    
    def execute(self,context):
        """This function has the definition necessary for creating Datasets for Exploratory Analysis"""
        postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)

        start = datetime.now()

        if self.table == 'FRAUD_AMOUNTS_METRIC':
            sql = DataAnalaysisOperator.fraud_amount_sql_template.format(
                destination_table = self.table
            )

        elif self.table == 'CARD_PRODUCT_DETAIL_METRIC':
            sql = DataAnalaysisOperator.card_product_sql_template.format(
                destination_table = self.table
            ) 
        
        logging.info(f"Creating table {self.table}")
        postgres.run(sql)
        logging.info(f"Created table {self.table}")
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")