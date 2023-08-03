WITH source AS (
    
    SELECT * FROM {{ source('rds', 'stop_status_log')}}

)

SELECT
    actual_duration
    , break_duration
    , created_at
    , delay
    , expected_duration
    , id
    , timestamp AS timestampp
    , status
    , stop_id 
    , updated_at
    , user_id
FROM source