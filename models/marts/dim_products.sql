with products as (

    select * from {{ ref('stg_products') }}

),

sales_stats as (

    select
        product_id,
        count(distinct order_id) as orders_count,
        sum(quantity) as total_units_sold,
        sum(net_revenue) as total_revenue,
        sum(gross_profit) as total_gross_profit,
        avg(discount_pct) as avg_discount_pct

    from {{ ref('int_order_items_enriched') }}
    group by 1

),

final as (

    select
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.unit_price,
        p.unit_cost,
        p.gross_margin,
        p.margin_pct,
        p.is_active,
        p.created_at,
        p.created_date,
        coalesce(ss.orders_count, 0)  as orders_count,
        coalesce(ss.total_units_sold, 0)  as total_units_sold,
        coalesce(ss.total_revenue, 0)  as total_revenue,
        coalesce(ss.total_gross_profit, 0)  as total_gross_profit,
        coalesce(ss.avg_discount_pct, 0)  as avg_discount_pct,
        case
            when coalesce(ss.total_revenue, 0) >= 300 then 'Top Seller'
            when coalesce(ss.total_revenue, 0) >= 100 then 'Mid Seller'
            when coalesce(ss.total_revenue, 0) > 0 then 'Low Seller'
            else 'No Sales'
        end as performance_tier,
        case p.category
            when 'electronics' then 'Tech'
            when 'apparel' then 'Fashion'
            when 'home & kitchen' then 'Home'
            when 'sports' then 'Sports'
            else 'Other'
        end as category_group
    from products p
    left join sales_stats ss
        on p.product_id = ss.product_id

)

select * from final
