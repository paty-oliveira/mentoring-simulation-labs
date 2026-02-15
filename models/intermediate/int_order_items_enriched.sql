with order_items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

enriched as (

    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        oi.quantity,
        oi.unit_price,
        oi.discount_pct,
        oi.gross_revenue,
        oi.net_revenue,
        oi.discount_amount,
        p.unit_cost * oi.quantity as total_cost,
        oi.net_revenue - (p.unit_cost * oi.quantity) as gross_profit,
        (oi.net_revenue - (p.unit_cost * oi.quantity)) / nullif(oi.net_revenue, 0) as gross_profit_margin

    from order_items oi
    left join products p
        on oi.product_id = p.product_id

)

select * from enriched
