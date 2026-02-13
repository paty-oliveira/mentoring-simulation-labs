with customers as (

    select * from {{ ref('stg_customers') }}

),

order_stats as (

    select
        customer_id,
        count(*)                                                        as total_orders,
        sum(net_revenue)                                                as lifetime_value,
        min(order_date)                                                 as first_order_date,
        max(order_date)                                                 as last_order_date,
        sum(case when order_status = 'delivered'    then 1 else 0 end) as delivered_orders,
        sum(case when order_status = 'returned'     then 1 else 0 end) as returned_orders,
        sum(case when order_status = 'cancelled'    then 1 else 0 end) as cancelled_orders

    from {{ ref('fct_orders') }}
    group by 1

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.country,
        c.phone,
        c.created_at,
        c.created_date,

        coalesce(os.total_orders,       0)  as total_orders,
        coalesce(os.lifetime_value,     0)  as lifetime_value,
        os.first_order_date,
        os.last_order_date,
        coalesce(os.delivered_orders,   0)  as delivered_orders,
        coalesce(os.returned_orders,    0)  as returned_orders,
        coalesce(os.cancelled_orders,   0)  as cancelled_orders,

        -- Customer value tier based on lifetime spend.
        -- NOTE: this classification logic is a great macro candidate!
        case
            when coalesce(os.lifetime_value, 0) >= 500  then 'Gold'
            when coalesce(os.lifetime_value, 0) >= 200  then 'Silver'
            when coalesce(os.lifetime_value, 0) >= 50   then 'Bronze'
            else                                              'New'
        end                                 as customer_tier,

        -- Customer engagement segment based on order count.
        -- NOTE: another repeated classification pattern — also a macro candidate!
        case
            when coalesce(os.total_orders, 0) = 0   then 'No Orders'
            when coalesce(os.total_orders, 0) = 1   then 'One-Time Buyer'
            when coalesce(os.total_orders, 0) <= 3  then 'Repeat Buyer'
            else                                          'Loyal Customer'
        end                                 as customer_segment

    from customers c
    left join order_stats os
        on c.customer_id = os.customer_id

)

select * from final
