FROM puckel/docker-airflow:1.10.9

USER root

# Установка нужных библиотек
RUN pip install --upgrade pip \
    && pip install pandas pyarrow sqlalchemy psycopg2-binary

USER airflow
