# data-ranger-poc


Steps:

1. docker-compose up airflow-init
2. docker-compose up -d

If there are problems, delete and rebuild:
- docker-compose down -v

If a new library has been added, rebuild:
- docker-compose up -d --build


3. Login into the airflow webserver
User: airflow
Psswd: airflow