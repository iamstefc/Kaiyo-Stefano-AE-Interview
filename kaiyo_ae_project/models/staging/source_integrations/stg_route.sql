WITH source AS (

    SELECT * FROM {{ ref('base_route') }}

),

source_rename AS (

    SELECT
        created_at AS route_actual_created_at
        , date AS route_expected_created_at
        , id AS route_id
        , updated_at AS route_actual_updated_at
        , origin_facility_id
        , route_type
        , status AS route_status
    FROM source

)

SELECT
    route_actual_created_at
    , route_expected_created_at
    , route_id
    , route_actual_updated_at
    , origin_facility_id
    , route_type
    , route_status
FROM source_rename
