WITH table_route_summary AS (

    SELECT * FROM {{ ref('int_route_summary') }}

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
FROM table_route_summary