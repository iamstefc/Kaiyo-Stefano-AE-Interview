WITH source AS (

    SELECT * FROM {{ ref('base_stop_types') }}

),

source_rename AS (

    SELECT
        created_at AS stop_created_at
        , name AS stop_types
    FROM source

)

SELECT
    stop_created_at
    , stop_types
FROM source_rename