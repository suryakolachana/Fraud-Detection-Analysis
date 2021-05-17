DROP TABLE Card_Product_detail_metric;

CREATE TABLE Card_Product_detail_metric as 
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