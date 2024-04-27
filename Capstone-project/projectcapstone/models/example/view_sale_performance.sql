select order_id,
order_purchase_timestamp,
product_id,
price
from {{ ref('model_olist_obt') }} 