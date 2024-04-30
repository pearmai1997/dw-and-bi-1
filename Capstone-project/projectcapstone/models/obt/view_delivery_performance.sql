select *,
case
    when num_of_delivered_date < 4 then "VERY FAST"
	when num_of_delivered_date >= 4 and num_of_delivered_date < 8 then "FAST"
	when num_of_delivered_date >= 8 and num_of_delivered_date < 15 then "NORMAL"
	when num_of_delivered_date >= 15 and num_of_delivered_date < 22 then "SLOW"
    when num_of_delivered_date is null then "-"
	else "VERY SLOW"
end as shipping_order_group,
from (select order_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_delivered_carrier_date,
    date_diff(order_delivered_customer_date, order_delivered_carrier_date, day) as num_of_delivered_date,
    order_approved_at,
    product_id,
    product_category_name,
from {{ ref('olist_obt') }} )
