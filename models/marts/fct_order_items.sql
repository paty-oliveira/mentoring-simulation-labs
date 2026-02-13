with order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_date,
        order_week,
        order_month,
        order_year,
        order_status,
        shipping_country,
        has_discount

    from {{ ref('fct_orders') }}

),

final as (

    select
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        oi.product_id,
        oi.product_name,
        oi.category,
        oi.subcategory,

        -- Category group — same CASE WHEN as in dim_products.sql (macro candidate!)
        case oi.category
            when 'electronics'      then 'Tech'
            when 'apparel'          then 'Fashion'
            when 'home & kitchen'   then 'Home'
            when 'sports'           then 'Sports'
            else                         'Other'
        end                             as category_group,

        o.order_date,
        o.order_week,
        o.order_month,
        o.order_year,
        o.order_status,
        o.shipping_country,
        o.has_discount,

        oi.quantity,
        oi.unit_price,
        oi.discount_pct,

        -- Revenue calculations — same formulas as in stg_order_items.sql (macro candidate!)
        oi.unit_price * oi.quantity                             as gross_revenue,
        oi.unit_price * oi.quantity * (1 - oi.discount_pct)    as net_revenue,
        oi.unit_price * oi.quantity * oi.discount_pct           as discount_amount,

        oi.total_cost,
        oi.gross_profit,
        oi.gross_profit_margin

    from order_items oi
    left join orders o
        on oi.order_id = o.order_id

)

select * from final
