import os

print("Setup Open Lakehouse platform ...")

os.system("docker build -t customize_airflow:latest ./Dockerfiles/Airflow")


os.system("docker compose up")

