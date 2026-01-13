{{
    config(materialized='materialized_view')
}}

{%- set relation = this %}
{%- set flattened_cols = flatten_json_column(relation, 'event_data') %}
{{ log(flattened_cols, info=True) }}

-- Deduplicate events: keep only the latest record per event_id
-- RisingWave automatically optimizes this to GroupTopNExecutor
SELECT 
    event_id,
    user_id,
    event_type,
    event_data,
    event_timestamp,
    user_properties,
    -- Extract nested fields from JSON for easier querying
    event_data->>'action' as event_data__action,
    (event_data->>'amount')::DECIMAL as event_data__amount,
    event_data->'metadata'->>'source' as event_data__metadata__source,
    event_data->'metadata'->>'campaign' as event_data__metadata__campaign,
    user_properties->>'device_type' as user_properties__device_type,
    user_properties->'location'->>'country' as user_properties__location__country,
    user_properties->'location'->>'city' as user_properties__location__city
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY event_timestamp DESC
        ) AS rn
    FROM user_events
)
WHERE rn = 1
