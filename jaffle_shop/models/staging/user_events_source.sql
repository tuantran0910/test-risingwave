{{
    config(materialized='source')
}}

CREATE SOURCE user_events_cdc WITH (
    connector = 'postgres-cdc',
    hostname = 'postgres-cdc',
    port = '5432',
    username = 'cdc_user',
    password = 'cdc_password',
    database.name = 'app_db',
    schema.name = 'public',
    slot.name = 'risingwave_slot',
    publication.name = 'rw_publication'
)
