select order_id,
order_purchase_timestamp,
price,
product_id,
product_category_name,
seller_id,
seller_zip_code_prefix,
seller_city,
seller_state
from {{ ref('olist_obt') }} 