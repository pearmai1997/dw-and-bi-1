select
    o.id as order_id
    , o.user_id as user_id
    , c.first_name
    , c.last_name
    , o.order_date
    , o.status as order_status

from {{ref('stg__jaffle_shope_orders')}} as o
join {{ref('stg__jaffle_shope_customers')}} as c
on o.user_id = c.id