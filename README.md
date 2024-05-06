# lakehouse-trino-mino-iceberg-airflow


# How to run ?
1. Clone this project

2. Build and run
```
python run.py
```

3. Go to  <b>localhost:8080</b>  to check Airflow dags

4. To check Minio:  <b> localhost:9001 </b>
- User: minio
- Password: minio123

5. You can also check Iceberg Table using Trino after finish the Airflow dags
- Port: 8181
- Database: default
- User: admin

8. Stop
```
docker compose down
```

9. Check file docx for details and explaination