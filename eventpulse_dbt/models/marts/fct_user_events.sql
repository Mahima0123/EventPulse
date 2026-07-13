SELECT
    event_id,
    user_id,
    session_id,
    event_type,
    content_id,
    country,
    subscription_type,
    event_timestamp
FROM {{ ref('int_user_sessions') }}
