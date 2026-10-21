with source as (

    select * from {{ source('weather', 'weather') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by city, observed_at
            order by ingested_at desc
        ) as row_num
    from source

),

renamed as (

    select
        observed_at,
        city,
        temperature_c,
        wind_speed_kmh,
        cloud_cover_pct,
        precipitation_mm,
        grid_latitude,
        grid_longitude,
        partition_date,
        ingested_at,
        source as source_name
    from deduplicated
    where row_num = 1

)

select * from renamed