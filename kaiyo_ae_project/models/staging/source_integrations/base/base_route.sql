WITH source AS (

    SELECT * FROM {{ source('rds', 'route')}}

)

SELECT
    created_at
    , date
    , id
    , updated_at
    , origin_facility_id
    , route_type
    , status
FROM source
