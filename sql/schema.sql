CREATE TABLE IF NOT EXISTS raw_user_events (
    event_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255),
    content_id VARCHAR(255),
    country VARCHAR(255),
    subscription_type VARCHAR(255),
    event_timestamp TIMESTAMP
)