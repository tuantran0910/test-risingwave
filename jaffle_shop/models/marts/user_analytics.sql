{{
    config(
        materialized='materialized_view',
        zero_downtime={'enabled': true}
    )
}}

WITH enriched_events AS (
    SELECT 
        e.*,
        -- User dimension joins
        u.username,
        u.email,
        u.registration_date,
        u.user_profile->>'subscription_tier' as subscription_tier,
        u.user_profile->'preferences'->>'language' as preferred_language,
        u.is_active as user_is_active,
        
        -- Product dimension joins (for purchase events)
        CASE 
            WHEN e.event_type = 'purchase' 
            THEN (e.event_data->>'product_id')::INT 
            ELSE NULL 
        END as product_id,
        p.product_name,
        p.category,
        p.price,
        p.product_specs->>'brand' as brand,
        p.product_specs->>'model' as model,
        
        -- Calculate derived metrics
        CASE WHEN e.event_type = 'purchase' THEN e.amount ELSE 0 END as revenue,
        CASE WHEN e.event_type = 'login' THEN 1 ELSE 0 END as login_count,
        CASE WHEN e.event_type = 'page_view' THEN 1 ELSE 0 END as page_view_count
        
    FROM user_events_deduplicated e
    LEFT JOIN users u ON e.user_id = u.user_id
    LEFT JOIN products p ON 
        CASE WHEN e.event_type = 'purchase' THEN (e.event_data->>'product_id')::INT ELSE NULL END = p.product_id
)

SELECT 
    -- User dimensions
    user_id,
    username,
    email,
    subscription_tier,
    preferred_language,
    country,
    city,
    device_type,
    
    -- Time dimensions
    DATE_TRUNC('day', event_timestamp) as event_date,
    DATE_TRUNC('hour', event_timestamp) as event_hour,
    
    -- Aggregation metrics
    COUNT(*) as total_events,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchase_count,
    COUNT(CASE WHEN event_type = 'login' THEN 1 END) as login_count,
    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) as page_view_count,
    
    -- Revenue metrics
    SUM(revenue) as total_revenue,
    AVG(CASE WHEN revenue > 0 THEN revenue END) as avg_order_value,
    
    -- Product category breakdown
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN product_id END) as unique_products_purchased,
    STRING_AGG(DISTINCT CASE WHEN event_type = 'purchase' THEN category END, ', ') as purchased_categories,
    
    -- Behavioral metrics
    MAX(event_timestamp) as last_activity,
    MIN(CASE WHEN event_type = 'login' THEN event_timestamp END) as first_login,
    MAX(CASE WHEN event_type = 'login' THEN event_timestamp END) as last_login,
    
    -- User status
    BOOL_AND(user_is_active) as currently_active,
    
    -- JSON aggregations for flexible analytics
    jsonb_object_agg(
        event_type, 
        COUNT(*)
    ) FILTER (WHERE event_type IS NOT NULL) as event_type_breakdown,
    
    jsonb_build_object(
        'total_sessions', COUNT(CASE WHEN event_type = 'login' THEN 1 END),
        'total_purchases', COUNT(CASE WHEN event_type = 'purchase' THEN 1 END),
        'total_page_views', COUNT(CASE WHEN event_type = 'page_view' THEN 1 END),
        'avg_session_duration', AVG(CASE WHEN event_type = 'session_end' THEN (event_data->>'duration_minutes')::FLOAT END),
        'preferred_device', mode() WITHIN GROUP (ORDER BY device_type)
    ) as user_metrics

FROM enriched_events
GROUP BY 
    user_id,
    username,
    email,
    subscription_tier,
    preferred_language,
    country,
    city,
    device_type,
    DATE_TRUNC('day', event_timestamp),
    DATE_TRUNC('hour', event_timestamp)
