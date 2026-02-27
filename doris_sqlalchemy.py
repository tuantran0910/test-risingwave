import logging
import os
from datetime import datetime

import dlt
from dlt.destinations import doris
from dlt.destinations.impl.doris.doris_adapter import doris_adapter

logging.basicConfig(level=logging.INFO)

os.environ["DLT_LOG_LEVEL"] = "INFO"

pipeline = dlt.pipeline(
    destination=doris(
        credentials={
            "host": "localhost",
            "port": 9030,
            "database": "raw",
            "username": "root",
            "password": "",
        },
    ),
    progress="log",
)


@dlt.resource(
    primary_key=["user_id"], write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def user_events_v5():
    for i in range(10, 15):
        yield {"user_id": i, "event_type": "register", "timestamp": datetime(2026, 2, 19)}


resource = doris_adapter(
    user_events_v5(),
    # table_engine="iceberg",
    # table_properties={"timeline.timestamp_column": "timestamp"},
)

# Run pipeline and capture result
result = pipeline.run(
    resource,
    dataset_name="raw",
    # loader_file_format="parquet",
    # refresh="drop_sources",
    # staging="filesystem",
)

# DEBUG: Check what's in the schema
print("=== SCHEMA DEBUG INFO ===")
print(f"Pipeline name: {pipeline.pipeline_name}")
print(f"Dataset name: {pipeline.dataset_name}")
print(f"Available schemas: {list(pipeline.schemas.keys())}")

# Get the schema
schema = list(pipeline.schemas.values())[0]  # Get first schema
print(f"Schema name: {schema.name}")
print(f"Schema tables: {list(schema.tables.keys())}")

# Find user_events table
for table_name, table in schema.tables.items():
    if "user_events" in table_name.lower():
        print(f"\n=== Table: {table_name} ===")
        print(f"  write_disposition: {table.get('write_disposition')}")

        # Check primary key columns
        pk_columns = []
        for col_name, col_props in table.get("columns", {}).items():
            if col_props.get("primary_key"):
                pk_columns.append(col_name)
        print(f"  primary_key columns: {pk_columns}")

        # Check for x-stage-data-deduplicated hint
        print(f"  x-stage-data-deduplicated: {table.get('x-stage-data-deduplicated')}")

        # List all hints
        hints = [
            k
            for k in table.keys()
            if k.startswith("x-") or k in ["primary_key", "write_disposition", "table_name"]
        ]
        print(f"  all table hints: {hints}")

print("\n=== RESULT INFO ===")
result_dict = result.asdict()
print(f"Loads IDs: {result_dict.get('loads_ids', 'N/A')}")
print(f"Dataset name: {result_dict.get('dataset_name', 'N/A')}")

print("\n=== LOAD PACKAGES ===")
for pkg_name in result_dict.get("load_packages", []):
    print(f"Package: {pkg_name}")
