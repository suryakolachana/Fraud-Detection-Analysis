select * from TRANSACTION_IDENTITIES LIMIT 10;

select * from TRANSACTIONS LIMIT 10;

-----I want list of fraudelent and legitimite transactions with their device types and device_info
select ti.device_type,ti.device_info,count(t.transaction_id) as Mobile_Fraudelent_Transactions
from transactions t,
     transaction_identities ti
where t.transaction_id = ti.transaction_id
and t.is_fraud = 1
and ti.device_type = 'mobile'
and ti.device_info is not null
group by ti.device_type,ti.device_info
order by Mobile_Fraudelent_Transactions desc;

select ti.device_type,ti.device_info,count(t.transaction_id) as Desktop_Fraudelent_Transactions
from transactions t,
     transaction_identities ti
where t.transaction_id = ti.transaction_id
and t.is_fraud = 1
and ti.device_type = 'desktop'
and ti.device_info is not null
group by ti.device_type,ti.device_info
order by Desktop_Fraudelent_Transactions desc;

select ti.device_type,ti.device_info,count(t.transaction_id) as Desktop_Fraudelent_Transactions
from transactions t,
     transaction_identities ti
where t.transaction_id = ti.transaction_id
and t.is_fraud = 1
and ti.device_type is null
and ti.device_info is null
group by ti.device_type,ti.device_info
order by Desktop_Fraudelent_Transactions desc;

----------------------------------------------------
select device_type, count(distinct device_info)
from transaction_identities
group by device_type;

---------------------------------------------------------------

select t.*
from transactions t,
     transaction_identities ti
where t.transaction_id = ti.transaction_id
and t.is_fraud = 1
and ti.device_type is null
and ti.device_info is null
and t.transaction_id = 3089999;

select count(*) from transactions 
where card1 = 13413 
and is_fraud = 1;

select * from transactions 
where card1 = 13413 
and is_fraud = 0;

select card1,card2,card3,card4,card5,card6,count(*) as legitimate_Transactions
from transactions 
--where card1 = 13413 
where is_fraud = 0
group by card1,card2,card3,card4,card5,card6
order by legitimate_Transactions desc,card1,card2,card3,card4,card5,card6;

select card1,card2,card3,card4,card5,card6,count(*) as Fraudelent_Transactions
from transactions 
where card1 = 13413 
and is_fraud = 1
group by card1,card2,card3,card4,card5,card6
order by Fraudelent_Transactions desc,card1,card2,card3,card4,card5,card6;

select count(*) as Fraudelent_Transactions
from transactions 
where is_fraud = 1;

select count(*) as Legitimate_Transactions
from transactions 
where is_fraud = 0;

select count(t.*) as Fraudelent_Transactions
from transactions t
where t.is_fraud = 1
and exists (select 1 from transaction_identities ti where ti.transaction_id = t.transaction_id);

select count(t.*) as Legitimate_Transactions
from transactions t
where t.is_fraud = 0
and exists (select 1 from transaction_identities ti where ti.transaction_id = t.transaction_id);

select count(t.*) as Fraudelent_Transactions
from transactions t
where t.is_fraud = 1
and not exists (select 1 from transaction_identities ti where ti.transaction_id = t.transaction_id);

select count(t.*) as Legitimate_Transactions
from transactions t
where t.is_fraud = 0
and not exists (select 1 from transaction_identities ti where ti.transaction_id = t.transaction_id);


-----------------------------------------------------------------------------------------------
1) Fraudelent Tranasactions vs legitimate Transaction percentage with out Identity Information 
2) Fraudelent Tranasactions with Identity Information vs legitimate Transaction with Identity Information percentage 
3) Timedelta from a given reference datetime (not an actual timestamp) Visualizing days from origin and associated total transaction amount
4) ### Transaction Amount : transaction payment amount in USD Distribution of Transaction amount by fraudulent vs legitimate transactions
5) ### C1~C14 variables dataframe : counting, such as how many addresses are found to be associated with the payment card,etc.
6) productcd_plt.columns = ['ProductCD','isFraud','TotalAmount','NumberofTransactions']
7) # Total Transaction amount of Fraudulent transactions vs legitimate transactions grouped by ProductCD
8) # Visualizing Number of transactions in each product category
9) ### card1 - card6 : payment card information, such as card type, card category, issue bank, country, etc.
# Visualizing  variables Card 1, Card2 and Card3 and Card5 and associated number of transactions
# Distribution of Transaction amount of Fraudulent transactions vs legitimate transactions grouped by card details
# Visualizing variables card4 and card6 
# Number of transactions for each Card type and network, grouped by transaction type(Fraudulent/legitimate)
# Total transaction amount for each Card type and network, grouped by transaction type(Fraudulent/legitimate)
### M1 ~ M9 (logical) : match, such as names on card and address, etc.
# Visualizing M1~ M9 values and associated number of transactions
# Visualizing M1~M9 values and associated total transaction amount
### addr1 and addr2 : Address
# Visualizing addr1 values and associated number of transactions
# Visualizing addr1 values and associated total transaction amount
# Visualizing addr2 values and associated number of transactions
# Visualizing addr2 values and associated total transaction amount
### P_emaildomain: purchaser email domain
# Visualizing purchaser email domain of approximately 99% of the transactions.
### R_emaildomain : recipient email domain
# Visualizing recipient email domain of approximately 99% of the transactions.
### Device Type : type of device used for transaction (mobile/desktop/Unknown)
### Device Info 
# Visualizing Device information of approximately 99% of dataset (Number of transactions > 100)