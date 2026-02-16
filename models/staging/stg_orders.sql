{% set date_splits = ['week', 'month', 'year'] %}

{% set active_order_status = ('placed', 'processing', 'shipped') %}

with source as (

    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        order_id::varchar as order_id,
        customer_id::varchar as customer_id,
        order_status::varchar as order_status,
        case order_status
            when 'placed' then 'Order Placed'
            when 'processing' then 'In Processing'
            when 'shipped' then 'Shipped'
            when 'delivered' then 'Delivered'
            when 'cancelled' then 'Cancelled'
            when 'returned' then 'Returned'
            else 'Unknown'
        end as order_status_label,
        case
            when order_status in {{ active_order_status }}
            then true
            else false
        end as is_active_order,
        upper(shipping_country)::varchar as shipping_country,
        discount_code::varchar as discount_code,
        case
            when discount_code is not null and discount_code != '' 
            then true
            else false
        end as has_discount,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        created_at::date as order_date,
        {% for split in date_splits -%}
            date_trunc('{{ split }}', created_at::date) as order_{{ split }} {% if not loop.last %},{% endif %}
        {% endfor -%}

    from source

)

select * from renamed
