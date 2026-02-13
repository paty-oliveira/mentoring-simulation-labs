with source as (

    select * from {{ ref('raw_products') }}

),

renamed as (

    select
        product_id::varchar             as product_id,
        product_name::varchar           as product_name,
        lower(category)::varchar        as category,
        lower(subcategory)::varchar     as subcategory,
        unit_price::float               as unit_price,
        unit_cost::float                as unit_cost,
        unit_price - unit_cost          as gross_margin,
        (unit_price - unit_cost)
            / nullif(unit_price, 0)     as margin_pct,
        is_active::boolean              as is_active,
        created_at::timestamp           as created_at,
        created_at::date                as created_date

    from source

)

select * from renamed
