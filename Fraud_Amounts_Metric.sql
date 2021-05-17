DROP TABLE IF EXISTS FRAUD_AMOUNTS_METRIC;

CREATE TABLE FRAUD_AMOUNTS_METRIC AS
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