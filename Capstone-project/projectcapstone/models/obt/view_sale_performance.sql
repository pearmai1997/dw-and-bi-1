select order_id,
order_purchase_timestamp,
price,
product_id,
product_category_name,
customer_id,
customer_zip_code_prefix,
customer_city,
customer_state
from {{ ref('olist_obt') }} 
