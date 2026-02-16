{%- set tiers = [
    {
        'name': 'Gold',
        'value': 500,
        'operator': '>='
    },
    {
        'name': 'Silver',
        'value': 200,
        'operator': '>='
    },
    {
        'name': 'Bronze',
        'value': 50,
        'operator': '>='
    }
] -%}

{%- set default_customer_tier = 'New' -%}

{%- set segments = [
    {
        'name': 'No Orders',
        'operator': '=',
        'value': 0
    },
    {
        'name': 'One-Time Buyer',
        'operator': '=',
        'value': 1
    },
    {
        'name': 'Repeat Buyer',
        'operator': '<=',
        'value': 5
    }
 ] -%}

{%- set default_customer_segment = 'Loyal Customer' -%}

with customers as (

    select * from {{ ref('stg_customers') }}

),

order_stats as (

    select
        customer_id,
        count(*) as total_orders,
        sum(net_revenue) as lifetime_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        sum(case when order_status = 'delivered' then 1 else 0 end) as delivered_orders,
        sum(case when order_status = 'returned' then 1 else 0 end) as returned_orders,
        sum(case when order_status = 'cancelled' then 1 else 0 end) as cancelled_orders

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
        coalesce(os.total_orders, 0) as total_orders,
        coalesce(os.lifetime_value, 0) as lifetime_value,
        os.first_order_date,
        os.last_order_date,
        coalesce(os.delivered_orders, 0) as delivered_orders,
        coalesce(os.returned_orders, 0) as returned_orders,
        coalesce(os.cancelled_orders, 0) as cancelled_orders,
        case
            {% for tier in tiers -%}
                when coalesce(os.lifetime_value, 0) {{ tier.operator }} {{ tier.value }} then '{{ tier.name }}' 
            {% endfor -%}
            else '{{ default_customer_tier }}'
        end as customer_tier,
        case
            {% for segment in segments -%}
                when coalesce(os.total_orders, 0) {{ segment.operator }} {{ segment.value }} then '{{ segment.name }}'
            {% endfor -%}
            else '{{ default_customer_segment }}'
        end as customer_segment

    from customers c
    left join order_stats os
        on c.customer_id = os.customer_id

)

select * from final
