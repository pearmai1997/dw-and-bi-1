{{ config(materialized='table') }}

with customers as (

    select *
    from `dataengineer-415510.order.olist_customers_dataset` as c
    join `dataengineer-415510.order.olist_geolocation_dataset` as geo on c.customer_zip_code_prefix = geo.geolocation_zip_code_prefix

),

orders as (

    select *
    from `dataengineer-415510.order.olist_orders_dataset`

),
orderitem as (

    select *
    from `dataengineer-415510.order.olist_items_dataset`

),
products as (

    select *
    from `dataengineer-415510.order.olist_products_dataset`

),
sellers as (

    select *
    from `dataengineer-415510.order.olist_sellers_dataset` as s
    join `dataengineer-415510.order.olist_geolocation_dataset` as geo on s.seller_zip_code_prefix = geo.geolocation_zip_code_prefix

),
productcate as (

    select *
    from `dataengineer-415510.order.product_category_name_translation`

),
olist_obt as (

    select o.order_id,
    o.order_purchase_timestamp,
    o.order_status,
    o.order_approved_at,
    oi.order_item_id,
    oi.product_id,
    pc.string_field_1,
    oi.price,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    c.geolocation_lat as customer_geolocation_lat,
    c.geolocation_lng as customer_geolocation_lng,
    oi.seller_id,
    oi.shipping_limit_date,
    s.geolocation_lat as seller_geolocation_lat,
    s.geolocation_lng as seller_geolocation_lng,
    s.seller_city,
    s.seller_state
    from orders as o 
    join orderitem as oi on o.order_id = oi.order_id
    join customers as c on c.customer_id = o.customer_id
    join products as p on p.product_id = oi.product_id
    join sellers as s on s.seller_id = oi.seller_id
    join productcate as pc on pc.string_field_1 = p.product_category_name

)

select * from olist_obt