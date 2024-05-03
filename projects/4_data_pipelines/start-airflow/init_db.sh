#/bin/bash
service postgresql start
su - postgres bash -c "psql  < /opt/airflow/setup_db.sql"
airflow db init
airflow connections delete 'airflow_db'
airflow connections add 'airflow_db' --conn-uri 'postgres://airflow:airflow@127.0.0.1:5432/airflow'
