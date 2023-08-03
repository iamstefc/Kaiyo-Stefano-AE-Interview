WITH source AS (

    SELECT * FROM {{ ref('base_stop') }}

),

source_rename AS (

    SELECT
        address_id
        , created_at AS stop_created_at
        , date AS stop_expected_created_at
        , id AS stop_id
        , route_id
        , status AS stop_status
        , stop_type
        , updated_at AS stop_updated_at
        , facility_id
    FROM source

)

SELECT
    address_id
    , stop_created_at
    , stop_expected_created_at
    , stop_id
    , route_id
    , stop_status
    , stop_type
    , stop_updated_at
    , facility_id
FROM source_rename