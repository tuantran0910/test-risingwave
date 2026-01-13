{{
    config(materialized='table_with_connector')
}}

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    price DECIMAL(10,2),
    product_specs JSONB,  -- Contains nested product specifications
    created_at TIMESTAMP
) FROM user_events_cdc TABLE 'public.products'
