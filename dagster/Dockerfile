FROM python:3.12-slim
WORKDIR /opt/dagster/app
RUN pip install dagster==1.13.18 dagster-webserver==1.13.18 dagster-postgres==0.29.18 \
    requests==2.34.2 pydantic==2.13.4 psycopg2-binary==2.9.12 python-dotenv==1.2.3 \
    dbt-core==1.12.2 dbt-postgres==1.11.0
COPY . .