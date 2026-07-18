SELECT
    user_id,

    MAX(country) AS country,

    MAX(subscription_type) AS subscription_type

FROM {{ ref('stg_user_events') }}

GROUP BY user_id