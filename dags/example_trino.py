#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG using TrinoOperator.
"""

from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from utils import (HIVE_BUCKET_NAME, ICEBERG_BUCKET_NAME, MINIO_CLIENT,
                   RAW_DATA_BUCKET_NAME, create_bucket,
                   remove_bucket_and_objects, upload_raw_data_to_s3)

with models.DAG(
    dag_id="trino_on_iceberg_lakehouse",
    schedule="@once",  # Override to match your needs
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["Trino"],
) as dag:

    @task()
    def clean_lakehouse_storage():
        from utils import (HIVE_BUCKET_NAME, ICEBERG_BUCKET_NAME,
                           RAW_DATA_BUCKET_NAME)

        remove_bucket_and_objects(MINIO_CLIENT, HIVE_BUCKET_NAME)

        remove_bucket_and_objects(MINIO_CLIENT, ICEBERG_BUCKET_NAME)

        remove_bucket_and_objects(MINIO_CLIENT, RAW_DATA_BUCKET_NAME)

    @task()
    def prepare_buckets_for_lakehouse():
        create_bucket(MINIO_CLIENT, ICEBERG_BUCKET_NAME)
        create_bucket(MINIO_CLIENT, HIVE_BUCKET_NAME)
        create_bucket(MINIO_CLIENT, RAW_DATA_BUCKET_NAME)

    @task()
    def upload_raw_data():
        upload_raw_data_to_s3(MINIO_CLIENT, RAW_DATA_BUCKET_NAME)

    trino_create_hive_schema = SQLExecuteQueryOperator(
        conn_id="trino_conn",
        task_id="create_hive_schema",
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS hive.staging_tables with (LOCATION = 's3a://hive-staging/staging_tables/')
        """,
        handler=list,
    )

    trino_create_iceberg_schema = SQLExecuteQueryOperator(
        conn_id="trino_conn",
        task_id="create_iceberg_schema",
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS iceberg.iceberg_tables with (LOCATION = 's3a://iceberg-lakehouse/iceberg_tables/')
        """,
        handler=list,
    )

    trino_create_hive_staging_table = SQLExecuteQueryOperator(
        conn_id="trino_conn",
        task_id="create_hive_staging_table_from_raw_csv_file",
        sql=["DROP TABLE IF EXISTS hive.staging_tables.products",
        """
        CREATE TABLE hive.staging_tables.products (
            product_id varchar,
            product_category_name varchar,
            product_name_lenght varchar,
            product_description_lenght varchar,
            product_photos_qty varchar,
            product_weight_g varchar,
            product_length_cm varchar,
            product_height_cm varchar,
            product_width_cm varchar
        ) with(
            skip_header_line_count = 1,
            external_location = 's3a://raw-data/products/',
            format = 'Csv'
        )
        """],
        handler=list,
    )

    trino_create_empty_iceberg_table = SQLExecuteQueryOperator(
        conn_id="trino_conn",
        task_id="create_empty_iceberg_table",
        sql=["DROP TABLE IF EXISTS iceberg.iceberg_tables.products",
        """
        CREATE TABLE iceberg.iceberg_tables.products (
            product_id varchar,
            product_category_name varchar,
            product_name_lenght int,
            product_description_lenght int,
            product_photos_qty int,
            product_weight_g int,
            product_length_cm int,
            product_height_cm int,
            product_width_cm int
        )
        """],
        handler=list,
    )

    trino_filter_then_insert_to_iceberg_table = SQLExecuteQueryOperator(
        conn_id="trino_conn",
        task_id="filter_and_transform_hive_table_to_insert_to_iceberg_table",
        sql=f"""
        INSERT INTO iceberg.iceberg_tables.products
        SELECT product_id,
            product_category_name,
            cast(product_name_lenght as INTEGER) as product_name_lenght,
            cast(product_description_lenght as INTEGER) as product_description_lenght,
            cast(product_photos_qty as INTEGER) as product_photos_qty,
            cast(product_weight_g as INTEGER) as product_weight_g,
            cast(product_length_cm as INTEGER) as product_length_cm,
            cast(product_name_lenght as INTEGER) as product_name_lenght,
            cast(product_width_cm as INTEGER) as product_width_cm
        FROM hive.staging_tables.products
        WHERE product_name_lenght != ''
            and product_description_lenght != ''
            and product_photos_qty != ''
            and product_weight_g != ''
            and product_length_cm != ''
            and product_name_lenght != ''
            and product_width_cm != ''
        """,
        handler=list,
    )

    clean_lakehouse = clean_lakehouse_storage()
    create_buckets = prepare_buckets_for_lakehouse()
    upload_csv_to_s3 = upload_raw_data()

    chain(
        clean_lakehouse,
        create_buckets,
        upload_csv_to_s3,
        [trino_create_hive_schema, trino_create_iceberg_schema],
        [trino_create_hive_staging_table, trino_create_empty_iceberg_table],
        trino_filter_then_insert_to_iceberg_table
    )
    # >> create_buckets >> prepare_data
