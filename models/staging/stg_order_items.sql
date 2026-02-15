with source as (

    select * from {{ ref('raw_order_items') }}

),

renamed as (

    select
        order_item_id::varchar as order_item_id,
        order_id::varchar as order_id,
        product_id::varchar as product_id,
        quantity::integer as quantity,
        unit_price::float as unit_price,
        discount_pct::float as discount_pct,
        unit_price * quantity as gross_revenue,
        unit_price * quantity * (1 - discount_pct) as net_revenue,
        unit_price * quantity * discount_pct as discount_amount

    from source

)

select * from renamed
