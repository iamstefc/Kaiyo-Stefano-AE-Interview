WITH source AS (
    
    SELECT * FROM {{ source('rds', 'stop_types')}}

)

SELECT
    created_at
    , name
FROM source