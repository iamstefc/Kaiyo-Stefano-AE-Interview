WITH table_route AS (

    SELECT * FROM {{ ref('stg_route') }}

),

table_stop AS (

    SELECT * FROM {{ ref('stg_stop') }}

),

table_stop_status_log AS (

    SELECT * FROM {{ ref('stg_stop_status_log') }}

), delivery_stops AS (

    SELECT
        r.route_id,
        s.stop_id,
        ss.stop_status_id,
        s.address_id,
        r.origin_facility_id,
        s.facility_id,
        ss.user_id,
        r.route_status,
        ss.stop_status AS current_stop_status,
        s.stop_type,
        r.route_created_at,
        ss.stop_created_at AS current_stop_created_at
    FROM table_route r
    LEFT JOIN table_stop s
    ON r.route_id = s.route_id
    LEFT JOIN table_stop_status_log ss
    ON s.stop_id = ss.stop_id
    WHERE r.route_type = 'Delivery' AND ss.stop_status IN ('On The Way', 'Arrived', 'On Premise', 'Completed')

),

ordered_table AS (

    SELECT
        *,
        LAG(current_stop_status) OVER (PARTITION BY route_id, stop_id ORDER BY current_stop_created_at) AS prev_stop_status,
        LAG(current_stop_created_at) OVER (PARTITION BY route_id, stop_id ORDER BY current_stop_created_at) AS prev_created_at
    FROM delivery_stops

), 

time_calculations AS (
    
    SELECT
        *,
        LPAD(FLOOR(EXTRACT(EPOCH FROM (current_stop_created_at::timestamp - prev_created_at::timestamp)) / 3600)::text, 2, '0') || ':' ||
        LPAD(FLOOR((EXTRACT(EPOCH FROM (current_stop_created_at::timestamp - prev_created_at::timestamp)) % 3600) / 60)::text, 2, '0') || ':' ||
        LPAD((EXTRACT(EPOCH FROM (current_stop_created_at::timestamp - prev_created_at::timestamp)) % 60)::text, 2, '0') AS time_difference,
        DATE_DIFF('min', prev_created_at::timestamp, current_stop_created_at::timestamp) AS minutes_difference,
        CASE WHEN prev_stop_status = 'On The Way' AND current_stop_status = 'Arrived' THEN 'Driving Time'
            WHEN prev_stop_status = 'Arrived' AND current_stop_status = 'On Premise' THEN 'Building Access'
            WHEN prev_stop_status = 'On Premise' AND current_stop_status = 'Completed' THEN 'Processing Time'
        ELSE 'Not Assigned' END AS stop_category
    FROM ordered_table
)

SELECT
    route_id,
    stop_id,
    stop_status_id,
    address_id,
    origin_facility_id,
    facility_id,
    user_id,
    route_status,
    route_created_at,
    prev_stop_status,
    prev_created_at,    
    current_stop_status,
    current_stop_created_at,
    time_difference
    minutes_difference,
    stop_type,
    stop_category
FROM time_calculations
WHERE prev_stop_status IS NOT NULL AND stop_category != 'Not Assigned'