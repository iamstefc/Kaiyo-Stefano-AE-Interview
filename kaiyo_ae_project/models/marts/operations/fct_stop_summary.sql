WITH table_stop_summary AS (

    SELECT * FROM {{ ref('int_stop_summary') }}

)

SELECT
    route_id
    , stop_id
    , stop_status_id
    , address_id
    , origin_facility_id
    , facility_id
    , user_id
    , route_status
    , route_created_at
    , prev_stop_status
    , prev_created_at 
    , current_stop_status
    , current_stop_created_at
    , time_difference
    , minutes_difference
    , stop_type
    , stop_category
FROM table_stop_summary