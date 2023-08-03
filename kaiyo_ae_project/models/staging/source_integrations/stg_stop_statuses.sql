WITH source AS (

    SELECT * FROM {{ ref('base_stop_statuses') }}

),

source_rename AS (

    SELECT
        created_at AS stop_status_created_at
        , name AS stop_status
    FROM source

)

SELECT
    stop_created_at
    , stop_status
FROM source_rename