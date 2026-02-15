with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('int_order_items_enriched') }}

),

order_totals as (

    select
        order_id,
        count(*)                as item_count,
        sum(quantity)           as total_units,
        sum(gross_revenue)      as gross_revenue,
        sum(net_revenue)        as net_revenue,
        sum(discount_amount)    as total_discount,
        sum(total_cost)         as total_cost,
        sum(gross_profit)       as gross_profit

    from order_items
    group by 1

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_week,
        o.order_month,
        o.order_year,
        o.order_status,
        case o.order_status
            when 'placed'       then 'Order Placed'
            when 'processing'   then 'In Processing'
            when 'shipped'      then 'Shipped'
            when 'delivered'    then 'Delivered'
            when 'cancelled'    then 'Cancelled'
            when 'returned'     then 'Returned'
            else                     'Unknown'
        end as order_status_label,
        o.is_active_order,
        o.shipping_country,
        o.discount_code,
        o.has_discount,
        coalesce(ot.item_count, 0)  as item_count,
        coalesce(ot.total_units, 0)  as total_units,
        coalesce(ot.gross_revenue, 0)  as gross_revenue,
        coalesce(ot.net_revenue, 0)  as net_revenue,
        coalesce(ot.total_discount, 0)  as total_discount,
        coalesce(ot.total_cost, 0)  as total_cost,
        coalesce(ot.gross_profit, 0)  as gross_profit,
        case when o.order_status = 'delivered'  then ot.net_revenue else 0 end as delivered_revenue,
        case when o.order_status = 'shipped'    then ot.net_revenue else 0 end as shipped_revenue,
        case when o.order_status = 'placed'     then ot.net_revenue else 0 end as placed_revenue,
        case when o.order_status = 'returned'   then ot.net_revenue else 0 end as returned_revenue,
        case when o.order_status = 'cancelled'  then ot.net_revenue else 0 end as cancelled_revenue

    from orders o
    left join order_totals ot
        on o.order_id = ot.order_id

)

select * from final
