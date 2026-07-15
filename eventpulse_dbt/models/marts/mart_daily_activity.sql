SELECT
    DATE(event_timestamp) AS activity_date,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS daily_active_users,
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(
        CASE
            WHEN event_type = 'video_play'
            THEN 1
            ELSE 0
        END
    ) AS videos_played,

    SUM(
        CASE
            WHEN event_type = 'subscription_purchase'
            THEN 1
            ELSE 0
        END
    ) AS subscriptions_purchased
FROM {{ ref('fct_user_events') }}
GROUP BY 1
ORDER BY 1