FROM python:3.9.7

RUN apt-get install wget
RUN pip install "pandas==1.3.4" "sqlalchemy==1.4.39" "psycopg2==2.8.6" "dask[complete]==2023.5.1"

WORKDIR /app
COPY ingest_data_1.py ingest_data_1.py

ENTRYPOINT [ "python", "ingest_data_1.py" ]