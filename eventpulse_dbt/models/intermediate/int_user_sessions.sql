WITH events AS (

    SELECT *
    FROM {{ ref('stg_user_events') }}

),

previous_events AS (

    SELECT
        *,
        LAG(event_timestamp) OVER (
            PARTITION BY user_id
            ORDER BY event_timestamp
        ) AS previous_event_timestamp

    FROM events

),

session_flags AS (

    SELECT
        *,
        CASE
            WHEN previous_event_timestamp IS NULL THEN 1
            WHEN event_timestamp - previous_event_timestamp > INTERVAL '30 minutes'
                THEN 1
            ELSE 0
        END AS new_session

    FROM previous_events

),

final AS (

    SELECT
        event_id,
        user_id,
        event_type,
        content_id,
        country,
        subscription_type,
        event_timestamp,

        SUM(new_session) OVER (
            PARTITION BY user_id
            ORDER BY event_timestamp
            ROWS UNBOUNDED PRECEDING
        ) AS session_id

    FROM session_flags

)

SELECT *
FROM final