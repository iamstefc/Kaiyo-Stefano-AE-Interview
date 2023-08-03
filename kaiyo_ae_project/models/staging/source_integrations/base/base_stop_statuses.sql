WITH source AS (
    
    SELECT * FROM {{ source('rds', 'stop_statuses')}}

)

SELECT
    created_at
    , name
FROM source