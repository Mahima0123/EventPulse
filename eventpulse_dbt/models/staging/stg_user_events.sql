with source as (
    select *
    from {{  source('raw', 'raw_user_events') }}
),
cleaned as (
    select distinct
        event_id,
        user_id,
        lower(trim(event_type)) as event_type,
        content_id,
        upper(trim(country)) as country,
        lower(trim(subscription_type)) as subscription_type,
        event_timestamp

    from source
    where event_id is not null
        and user_id is not null
        and event_timestamp is not null
)

select *
from cleaned