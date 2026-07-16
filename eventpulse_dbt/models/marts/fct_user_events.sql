{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

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

{% if is_incremental() %}
WHERE event_timestamp > 
(
    SELECT COALESCE(MAX(event_timestamp), '1900-01-01') 
    FROM {{ this }}
)
{% endif %}