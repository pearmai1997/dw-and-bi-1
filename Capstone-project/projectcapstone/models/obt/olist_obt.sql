{{ config(materialized='table') }}

with customers as (

    select *
    from `dataengineer-415510.order.olist_customers_dataset` as c

),

orders as (

    select o.order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price
    from `dataengineer-415510.order.olist_orders_dataset` as o
    join `dataengineer-415510.order.olist_items_dataset` as oi on o.order_id = oi.order_id

),
products as (

    select *
    from `dataengineer-415510.order.olist_products_dataset` as p
    left join `dataengineer-415510.order.product_category_name_translation` as pc
    on p.product_category_name = pc.product_category_name

),
sellers as (

    select *
    from `dataengineer-415510.order.olist_sellers_dataset` as s

),

olist_obt as (

    select o.order_id,
    o.order_purchase_timestamp,
    o.order_status,
    o.order_approved_at,
    o.order_item_id,
    o.product_id,
    p.product_category_name_english as product_category_name,
    o.price,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    o.seller_id,
    o.shipping_limit_date,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,

    from orders as o 
    join customers as c on c.customer_id = o.customer_id
    left join products as p on p.product_id = o.product_id
    join sellers as s on s.seller_id = o.seller_id

)

select * from olist_obt 
where order_purchase_timestamp between "2017-01-01" and "2018-08-30"
order by order_purchase_timestamp