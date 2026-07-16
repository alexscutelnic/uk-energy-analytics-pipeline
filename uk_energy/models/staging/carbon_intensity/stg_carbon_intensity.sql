with source as (

    select * from {{ source('carbon_intensity', 'carbon_intensity') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by from_time
            order by ingested_at desc
        ) as row_num
    from source

),

renamed as (

    select
        from_time                   as reading_from_at,
        to_time                     as reading_to_at,
        intensity_forecast          as forecast_gco2_per_kwh,
        intensity_actual            as actual_gco2_per_kwh,
        lower(intensity_index)      as intensity_band,
        ingested_at,
        source                      as source_url
    from deduplicated
    where row_num = 1

)

select * from renamed