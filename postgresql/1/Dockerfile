FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 fastparquet pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py 

# overide python prompt to bash 
# ENTRYPOINT [ "bash" ]

# use python prompt and run .py
ENTRYPOINT [ "python", "ingest_data.py" ]