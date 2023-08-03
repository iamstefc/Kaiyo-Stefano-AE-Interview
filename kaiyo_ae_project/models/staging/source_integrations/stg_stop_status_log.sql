WITH source AS (

    SELECT * FROM {{ ref('base_stop_status_log') }}

),

source_rename AS (

    SELECT
        actual_duration
        , break_duration
        , COALESCE(created_at, timestampp) AS stop_created_at
        , delay AS possible_delay_duration
        , expected_duration
        , id AS stop_status_id
        , status AS stop_status
        , stop_id 
        , updated_at AS stop_updated_at
        , user_id
    FROM source

)

SELECT
    actual_duration
    , break_duration
    , stop_created_at
    , possible_delay_duration
    , expected_duration
    , stop_status_id
    , stop_status
    , stop_id
    , stop_updated_at
    , user_id
FROM source_rename