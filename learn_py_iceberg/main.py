from datetime import datetime
from typing import List

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    TimestampType,
)


def create_sample_table() -> None:
    try:
        # 1. Define the schema
        schema = Schema(
            NestedField(1, "id", LongType(), True),
            NestedField(2, "name", StringType(), True),
            NestedField(3, "created_at", TimestampType(), False),
        )

        # 2. Configure and load the catalog
        catalog = load_catalog(
            "demo",
            **{
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "s3://warehouse/",
                "s3.endpoint": "http://localhost:9000",  # MinIO endpoint
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.path-style-access": "true",  # Important for MinIO
                "region": "us-east-1"
            }
        )

        # 3. Create the table
        catalog.create_table_if_not_exists(
            identifier="my_namespace.my_table",
            schema=schema,
            properties={"format-version": "2"}
        )

        # 4. Write some data
        table = catalog.load_table("my_namespace.my_table")

        data: List[dict] = [
            {
                "id": 1,
                "name": "Example 1",
                "created_at": datetime.now()
            },
            {
                "id": 2,
                "name": "Example 2",
                "created_at": datetime.now()
            }
        ]

        data_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("created_at", pa.timestamp("us"))
        ])

        df = pa.Table.from_pylist(data, schema=pa.schema(data_schema))
        table.append(df)

        # 5. Read data
        df = table.scan().to_pandas()
        print(f"Table contents:\n{df}")

        with table.update_schema() as update:
            update.add_column("deleted_at", TimestampType(), required=False)

    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    create_sample_table()
