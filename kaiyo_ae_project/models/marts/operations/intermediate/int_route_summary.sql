WITH table_route AS (

    SELECT * FROM {{ ref('stg_route') }}

),

table_stop AS (

    SELECT * FROM {{ ref('stg_stop') }}

),

table_stop_status_log AS (

    SELECT * FROM {{ ref('stg_stop_status_log') }}

), 

route_stop_summary AS (

    SELECT
        r.route_id
        , s.stop_id
        , ss.stop_status_id
        , ss.stop_created_at,
        COUNT(CASE WHEN r.route_status = 'Completed' THEN 1 END) OVER (PARTITION BY r.id) AS route_completed_count,
        COUNT(CASE WHEN r.route_status = 'Pending' THEN 1 END) OVER (PARTITION BY r.id) AS route_pending_count,
        COUNT(CASE WHEN r.route_status = 'Cancelled' THEN 1 END) OVER (PARTITION BY r.id) AS route_cancelled_count,
        COUNT(CASE WHEN ss.stop_status = 'Front Office Rescheduled' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS front_office_rescheduled_count,
        COUNT(CASE WHEN ss.stop_status = 'Front Office Cancelled' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS front_office_cancelled_count,
        COUNT(CASE WHEN ss.stop_status = 'Not Started' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS not_started_count,
        COUNT(CASE WHEN ss.stop_status = 'Waiting for customer' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS waiting_for_customer_count,
        COUNT(CASE WHEN ss.stop_status = 'Completed' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS completed_count,
        COUNT(CASE WHEN ss.stop_status = 'Front Office Merged' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS front_office_merged_count,
        COUNT(CASE WHEN ss.stop_status = 'On The Way' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS on_the_way_count,
        COUNT(CASE WHEN ss.stop_status = 'Arrived' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS arrived_count,
        COUNT(CASE WHEN ss.stop_status = 'On Premise' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS on_premise_count,
        COUNT(CASE WHEN ss.stop_status = 'Truck Cancelled' THEN 1 END) OVER (PARTITION BY r.id, s.id) AS truck_cancelled_count
    FROM rds.route r
    LEFT JOIN rds.stop s
    ON r.id = s.route_id
    LEFT JOIN rds.stop_status_log ss
    ON s.id = ss.stop_id
    WHERE r.route_type = 'Delivery'

),

stop_aggregated AS (

    SELECT
        route_id,
        stop_id,
        SUM(route_completed_count) AS route_completed_total,
        SUM(route_pending_count) AS route_pending_total,
        SUM(route_cancelled_count) AS route_cancelled_total,
        SUM(front_office_rescheduled_count) AS front_office_rescheduled_total,
        SUM(front_office_cancelled_count) AS front_office_cancelled_total,
        SUM(not_started_count) AS not_started_total,
        SUM(waiting_for_customer_count) AS waiting_for_customer_total,
        SUM(completed_count) AS completed_total,
        SUM(front_office_merged_count) AS front_office_merged_total,
        SUM(on_the_way_count) AS on_the_way_total,
        SUM(arrived_count) AS arrived_total,
        SUM(on_premise_count) AS on_premise_total,
        SUM(truck_cancelled_count) AS truck_cancelled_total,
        MAX(created_at) AS stop_last_recorded_at,
        MIN(created_at) AS stop_first_recorded_at
    FROM route_stop_summary
),

route_aggregated AS (

    SELECT
        route_id,
        COUNT(DISTINCT stop_id) AS total_stops,
        COUNT(stop_status_id) AS total_stop_status
    FROM route_stop_summary
),

route_summary AS (

    SELECT 
        sa.route_id,
        sa.stop_id,
        sa.route_completed_total,
        sa.route_pending_total,
        sa.route_cancelled_total,
        sa.front_office_rescheduled_total,
        sa.front_office_cancelled_total,
        sa.not_started_total,
        sa.waiting_for_customer_total,
        sa.completed_total,
        sa.front_office_merged_total,
        sa.on_the_way_total,
        sa.arrived_total,
        sa.on_premise_total,
        sa.truck_cancelled_total,
        sa.stop_last_recorded_at,
        sa.stop_first_recorded_at,
        ra.total_stops,
        ra.total_stop_status
    FROM stop_aggregated sa
    LEFT JOIN route_aggregated ra
    ON sa.route_id = ra.route_id
)

SELECT
    route_id,
    stop_id,
    route_completed_total,
    route_pending_total,
    route_cancelled_total,
    front_office_rescheduled_total,
    front_office_cancelled_total,
    not_started_total,
    waiting_for_customer_total,
    completed_total,
    front_office_merged_total,
    on_the_way_total,
    arrived_total,
    on_premise_total,
    truck_cancelled_total,
    stop_last_recorded_at,
    stop_first_recorded_at,
    total_stops,
    total_stop_status
FROM route_summary