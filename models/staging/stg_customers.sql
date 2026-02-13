with source as (

    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        customer_id::varchar            as customer_id,
        first_name::varchar             as first_name,
        last_name::varchar              as last_name,
        first_name || ' ' || last_name  as full_name,
        lower(email)::varchar           as email,
        upper(country)::varchar         as country,
        phone::varchar                  as phone,
        created_at::timestamp           as created_at,
        created_at::date                as created_date

    from source

)

select * from renamed
