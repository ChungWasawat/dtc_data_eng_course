from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.bigquery import BigQueryWarehouse


########################################## load data from web to gbucket
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, colour: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if colour == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif colour == "green":
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df    


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    local_path = Path.cwd()
    path = Path(f"data/{color}/{dataset_file}.parquet")
    path2 = local_path / path

    df.to_parquet(path2, compression="gzip")
    return path2


@task()
def write_gcs(path: Path, colour: str, dataset_file: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtcde-prefect-gcs")
    gBucket_path = f"data/{colour}/{dataset_file}.parquet"
    gcs_block.upload_from_path(from_path=path, to_path=gBucket_path)
    return

############################################# load data from bucket to bq
@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dtcde-prefect-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq_table(df: pd.DataFrame, colour: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtcde-prefect-gcp-creds")
    if colour == "yellow":
        dest_table = "eu_dtcDE_zoomcamp.yellow"
    elif colour == "green":
        dest_table = "eu_dtcDE_zoomcamp.green"
    elif colour == "fhv":
        dest_table = "eu_dtcDE_zoomcamp.fhv"

    df.to_gbq(
        destination_table=dest_table,
        project_id="mp-dtc-data-eng",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@task()
def write_bq_xtable(colour: str) -> None:
    """Write DataFrame to BiqQuery as external table"""
    with BigQueryWarehouse.load("bigquery") as warehouse:
        create_operation = '''
        CREATE TABLE IF NOT EXISTS mydataset.mytable (
            col1 STRING,
            col2 INTEGER,
            col3 BOOLEAN
        )
        '''
        warehouse.execute(create_operation)
        insert_operation = '''
        INSERT INTO mydataset.mytable (col1, col2, col3) VALUES (%s, %s, %s)
        '''
        seq_of_parameters = [
            ("a", 1, True),
            ("b", 2, False),
        ]
        warehouse.execute_many(
            insert_operation,
            seq_of_parameters=seq_of_parameters
        )


############################################# flow
@flow()
def etl_web_to_bq(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    # web to gcs
    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)

    # gcs to bq
    #path2 = extract_from_gcs(color, year, month)
    #df2 = transform(path2)
    #write_bq_table(df2, color)
    write_bq_xtable(color)

@flow()
def etl_parent_w2bq_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_bq(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_w2bq_flow(months, year, color)