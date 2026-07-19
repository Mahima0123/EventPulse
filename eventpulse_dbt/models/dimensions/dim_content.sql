SELECT
    content_id,

    COUNT(*) AS total_events

FROM {{ ref('stg_user_events') }}

WHERE content_id IS NOT NULL

GROUP BY content_id