import os

from dotenv import load_dotenv
from minio import Minio

load_dotenv()
LOCAL_FILE_PATH = (
    "/opt/airflow/raw-data/"
)
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"



MINIO_CLIENT = Minio(
    "minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
)

ICEBERG_BUCKET_NAME = "iceberg-lakehouse"
HIVE_BUCKET_NAME = "hive-staging"
RAW_DATA_BUCKET_NAME = "raw-data"


def remove_bucket_and_objects(client: Minio, bucket_name: str) -> None:
    found = client.bucket_exists(bucket_name)

    print("bucket name: ", bucket_name)
    if found:
        objects_deep = client.list_objects(bucket_name, recursive=True)

        for obj in objects_deep:
            print(obj.object_name)
            client.remove_object(bucket_name, obj.object_name)  # type: ignore

        objects = client.list_objects(bucket_name)

        for obj in objects:
            print(obj.object_name)
            client.remove_object(bucket_name, obj.object_name)  # type: ignore

        client.remove_bucket(bucket_name)


def create_bucket(client: Minio, bucket_name: str) -> None:

    print("bucket name: ", bucket_name)

    found = client.bucket_exists(bucket_name)

    if not found:
        client.make_bucket(bucket_name)


def upload_raw_data_to_s3(client: Minio, bucket_name: str):

    print("bucket name: ", bucket_name)

    found = client.bucket_exists(bucket_name)
    if found:
        print("Bucket already exists")

        raw_files = os.listdir(LOCAL_FILE_PATH)
        print(raw_files)

        renamed_files = [
            "_".join(file_name.split("_")[1:-1])
            + "/"
            + "_".join(file_name.split("_")[1:-1])
            + ".csv"
            for file_name in raw_files
        ]
        print(renamed_files)
        for raw_file, target_file in zip(raw_files, renamed_files):
            client.fput_object(
                "raw-data",
                target_file,
                LOCAL_FILE_PATH + raw_file,  # type: ignore
            )
        print("It is successfully uploaded to bucket")