FROM python:3.9.1

RUN apt-get install wget
RUN pip install sqlalchemy psycopg2 "dask[complete]"

WORKDIR /app
COPY ingest_data_1.py ingest_data_1.py

ENTRYPOINT [ "python", "ingest_data_1.py" ]