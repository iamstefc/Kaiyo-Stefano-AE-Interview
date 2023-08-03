WITH source AS (
    
    SELECT * FROM {{ source('rds', 'stop')}}

)

SELECT
    address_id
    , created_at
    , date
    , id
    , route_id
    , status
    , stop_type
    , updated_at
    , facility_id
FROM source