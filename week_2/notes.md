# Week 2: Workflow Orchestration

Pls see [README.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration) for week 2 from DataTalksClub GitHub repo.

Below are my notes from week 2.

## Contents

- 2.1 Data Lake
- 2.2 Introduction to Workflow Orchestration
- 2.3 Introduction to Prefect concepts
- 2.4 ETL with GCP & Prefect
- 2.5 From Google Cloud Storage to Big Query
- 2.6 Parametrizing Flow & Deployments
- 2.7 Schedules and Docker Storage with Infrastructure
- 2.8 Prefect Cloud and Additional Resources

## 2.1 Data Lake  

See [DE Zoomcamp 2.1.1 - Data Lake](https://www.youtube.com/watch?v=W3Zm6rjOq70) on Youtube.  

![Alt text](imgs/image.png)  

- It is a central repository that holds big data from many sources.  
- Generally, the data can be structured, semi-structured or unstructured, so the ideal is to ingest data as quickly as possible and make it available or accessible to other team members like DS, DA, DE.  

### Data Lake vs Data Warehouse

![Alt text](imgs/image-1.png)  

### How did Data Lake start?  

- Companies realized the value of data  
- Store and access data quickly  
- Cannot always define structure of data  
- Usefulness of data being realized later in the project lifecycle  
- Increase in data scientists  
- R&D on data products  
- Need for Cheap storage of Big data  

### ETL vs ELT  

- Extract Transform and Load (ETL) vs Extract Load and Transform (ELT)  
- ETL is mainly used for a small amount of data whereas ELT is used for large amounts of data  
- ELT provides data lake support (Schema on read)  

### Gotchas of Data Lake

- Converting into Data Swamp
- No versioning
- Incompatible schemas for same data without versioning
- No metadata associated
- Joins not possible  

### Cloud provider Data Lake

- GCP - cloud storage
- AWS - S3
- AZURE - AZURE BLOB

## 2.2 Introduction to Workflow orchestration

See [DE Zoomcamp 2.2.1 - Introduction to Workflow Orchestration](https://www.youtube.com/watch?v=8oLs6pzHp68) on
Youtube.  

### What is workflow orchestration?

It means governing your data flow in a way that respects orchestration rules and your business logic.  
So, what is the data flow? --> It is what bins and otherwise disparate set of application together. A data flow represents the movement of data from one component or system to another. The data flow may also be described as the transport of data from a source to a destination. An ETL (extract, transform, load) process is a type of data flow.  

**Dataflow concept:**

![Alt text](imgs/image-4.png)  

**Workflow Orchestration concept:**

![Alt text](imgs/image-2.png)

![Alt text](imgs/image-3.png)

### Core features of Workflow orchestration:

- Remote execution
- Scheduling
- Retries
- Caching
- Integration with external systems (APIs, Databases)
- Ad-hoc run
- Parameterization
- Alerts you when something fails

### Some Workflow Orchestration tools:

- Airflow
- Prefect

## 2.3 Introduction to Prefect concepts

See [DE Zoomcamp 2.2.2 - Introduction to Prefect concepts](https://www.youtube.com/watch?v=jAwRCyGLKOY) on Youtube and [GitHub repository](https://github.com/discdiver/prefect-zoomcamp)

We need a python requirements file.
File `requirements.txt`
```
pandas==1.5.2
pydantic==1.10.0
prefect==2.7.7
prefect-sqlalchemy==0.2.2
prefect-gcp[cloud_storage]==0.2.4
protobuf==4.21.11
pyarrow==10.0.1
pandas-gbq==0.18.1
psycopg2-binary==2.9.5
sqlalchemy==1.4.46
```
#### Ingestion without prefect
**File** `ingest-data.py`
```
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def ingest_data(user, password, host, port, db, table_name, url):
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    user = "postgres"
    password = "admin"
    host = "localhost"
    port = "5433"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)
```

First of all, open new terminal in IDEA, run this command to install library:
```
pip install -r requirements.txt
```
When completed, you can check by this command: `prefect version`

I started Docker and executed these commands.
```
mkdir ny_taxi_postgres_data
docker run -d \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```
Next, I executed the ingest_data.py   

```python ingest_data.py```   

Open pgcli:

```commandline
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

And run these SQL command:

```
root@localhost:ny_taxi> \dt
+--------+-------------------+-------+-------+
| Schema | Name              | Type  | Owner |
|--------+-------------------+-------+-------|
| public | yellow_taxi_trips | table | root  |
+--------+-------------------+-------+-------+
SELECT 1
Time: 0.006s
root@localhost:ny_taxi> select COUNT(*) from yellow_taxi_trips
+---------+
| count   |
|---------|
| 1369765 |
+---------+
SELECT 1
Time: 0.050s
root@localhost:ny_taxi>
```

#### Ingestion with prefect  
I create a new file python `ingest_data_flow.py` for using prefect
**File** `ingets_data_flow.py`
```
import os
import argparse
from datetime import timedelta
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df


@task(log_prints=True, retries=3)
def transform_data(df):
    print(f"Pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"Post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Subflow", log_prints=True)
def log_subflow(tablename: str):
    print(f"Logging Subflow for: {tablename}")

@flow(name='Ingest Flow')
def main_flow(table_name: str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == '__main__':
    main_flow("yellow_taxi_trips")
```

Start the Prefect Orion orchestration engine.  
```commandline
prefect orion start
```

Go to http://127.0.0.1:4200/ to open the Orion interface.

You can consult a lot of information about our performances.  

We are interested in **Blocks**.

We go to https://docs.prefect.io/collections/catalog/

See also https://prefecthq.github.io/prefect-sqlalchemy/

But the instructor suggests instead to add the block **SQLAlchemy Connector** from the interface **Blocks** of Orion.  

![img.png](imgs/img.png)  

Write or select these parameters :

- **Block Name**: postgres-connector  
- **Driver**: SyncDriver  
- **The driver name to use**: postgresql+psycopg2  
- **The name of the database to use**: ny_taxi  
- **Username**: root  
- **Password**: root  
- **Host**: localhost  
- **Port**: 5432  

Then click on the **Create** button  
![img_1.png](imgs/img_1.png)  

Then, I executed the python program.  

```
python ingest_data_flow.py
```

Opne `pgcli` and run sql command to view result.

### 2.4 ETL with GCP & Prefect   
See [DE Zoomcamp 2.2.3 - ETL with GCP & Prefect](https://www.youtube.com/watch?v=W-rMz_2GwqQ) on Youtube and the [source code](https://github.com/discdiver/prefect-zoomcamp/tree/main/flows/02_gcp).  

#### Start Prefect Orion  

```
prefect orion start
```

You should be see in the terminal this:
```

 ___ ___ ___ ___ ___ ___ _____    ___  ___ ___ ___  _  _
| _ \ _ \ __| __| __/ __|_   _|  / _ \| _ \_ _/ _ \| \| |
|  _/   / _|| _|| _| (__  | |   | (_) |   /| | (_) | .` |
|_| |_|_\___|_| |___\___| |_|    \___/|_|_\___\___/|_|\_|

Configure Prefect to communicate with the server with:

    prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

View the API reference documentation at http://127.0.0.1:4200/docs

Check out the dashboard at http://127.0.0.1:4200
```

Then, we can check the dashboard here: http://127.0.0.1:4200  

Create the new folder `02_gcp`, and create a new file with name `etl_web_to_gcs.py`  

### Create a bucket

Go to [Gougle Cloud Console](https://console.cloud.google.com/).

In the **DTC-DE** project, select **Cloud Storage**, and select **Buckets**. I already have a backup called
**dtc_data_lake_dtc-392100**. The instructor uses a bucket named **prefect-de-zoomcamp**.

Inside Orion, select **Blocks** at the left menu, choose the block **GCS Bucket** and click **Add +** button. Complete
the form with:

- Block Name: zoom-gcs

- Name of the bucket: dtc_data_lake_dtc-392100  

![img_2.png](imgs%2Fimg_2.png)  

### Modify our python program

We then obtain a fragment of code to insert into our python code. Which allows us to add the `write_gcs` method to
`etl_web_to_gcs.py`.  

**File `etl_web_to_gcs.py`**

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True, retries=3)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True, retries=3)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Wirte DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True, retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()
```

### Run the python program again

Let’s run the python program again.

``` bash
python flows/02_gcp/etl_web_to_gcs.py
```

```txt
(venv) (base) sang@sang-desktop:~/Desktop/Learning/DE/DTC/week_2$ python flows/02_gcp/etl_web_to_gcs.py 
09:14:52.147 | INFO    | prefect.engine - Created flow run 'robust-seal' for flow 'etl-web-to-gcs'
09:14:52.298 | INFO    | Flow run 'robust-seal' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
09:14:52.299 | INFO    | Flow run 'robust-seal' - Executing 'fetch-b4598a4a-0' immediately...
/home/sang/Desktop/Learning/DE/DTC/week_2/flows/02_gcp/etl_web_to_gcs.py:10: DtypeWarning: Columns (6) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
09:14:56.079 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
09:14:56.106 | INFO    | Flow run 'robust-seal' - Created task run 'clean-b9fd7e03-0' for task 'clean'
09:14:56.107 | INFO    | Flow run 'robust-seal' - Executing 'clean-b9fd7e03-0' immediately...
09:14:56.532 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID tpep_pickup_datetime  ... total_amount  congestion_surcharge
0       1.0  2021-01-01 00:30:10  ...         11.8                   2.5
1       1.0  2021-01-01 00:51:20  ...          4.3                   0.0

[2 rows x 18 columns]
09:14:56.533 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
09:14:56.535 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 1369765
09:14:56.570 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
09:14:56.595 | INFO    | Flow run 'robust-seal' - Created task run 'write_local-f322d1be-0' for task 'write_local'
09:14:56.596 | INFO    | Flow run 'robust-seal' - Executing 'write_local-f322d1be-0' immediately...
09:14:59.564 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
09:14:59.585 | INFO    | Flow run 'robust-seal' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
09:14:59.586 | INFO    | Flow run 'robust-seal' - Executing 'write_gcs-1145c921-0' immediately...
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
09:15:00.134 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
09:15:00.134 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'dtc_data_lake_dtc-392100'.
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
09:15:00.632 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
09:15:02.851 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-01.parquet') to the bucket 'dtc_data_lake_dtc-392100' path 'data/yellow/yellow_tripdata_2021-01.parquet'.
09:15:09.518 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
09:15:09.551 | INFO    | Flow run 'robust-seal' - Finished in state Completed('All states completed.')
```

### Bucket is created!

By checking on Google Cloud, we should see our bucket of 20.7 Mb. Congratulation!  

![img_3.png](imgs%2Fimg_3.png)


### 2.5 From Google Cloud Storage to Big Query

See [DE Zoomcamp 2.2.4 - From Google Cloud Storage to Big Query](https://www.youtube.com/watch?v=Cx5jt-V5sgE) on
Youtube.

Now let’s create another python program to load our data into the Google Cloud Storage (GCS) to Big Query.  

File `etl_gcs_to_bq.py`

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(log_prints=True, retries=3)
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning"""
    df = pd.read_parquet(path)
    print(f"Pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"Post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@flow()
def etl_gcs_to_bq():
    """main ETL flow to load data into Big Query"""
    color='yellow'
    year=2021
    month=1

    path = extract_from_gcs(color,year,month)
    df = transform(path)

if __name__ == '__main__':
    etl_gcs_to_bq()
```

Run that file by this command: `python flows/02_gcp/etl_gcs_to_bq.py`  

We should have result like this:
```
09:47:46.019 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Finished in state Completed()
09:47:46.043 | INFO    | Flow run 'zircon-goshawk' - Created task run 'transform-a7d916b4-0' for task 'transform'
09:47:46.043 | INFO    | Flow run 'zircon-goshawk' - Executing 'transform-a7d916b4-0' immediately...
09:47:46.215 | INFO    | Task run 'transform-a7d916b4-0' - Pre: missing passenger count: 98352
09:47:46.223 | INFO    | Task run 'transform-a7d916b4-0' - Post: missing passenger count: 0
09:47:46.247 | INFO    | Task run 'transform-a7d916b4-0' - Finished in state Completed()
09:47:46.276 | INFO    | Flow run 'zircon-goshawk' - Finished in state Completed('All states completed.')
```

Go to **Google Cloud Console**, select **Big Query**, click on **+ ADD DATA** button.  

The field **Create table from** should be set to **Google Cloud Storage**.

On **Select the file from GCS bucket**, click on **BROWSE** and select the `.parquet` file.

Under **Destination** section, click on **CREATE NEW DATASET** with the field **Dataset ID** equal to **dezoomcamp**.   
Then click on CREATE DATASET button.  

![img_4.png](imgs%2Fimg_4.png)  

Still under **Destination** section, name the table **rides**.

Then click on **CREATE TABLE** button.

Select the table rides, open a new Query tab, and run this query:  
```
SELECT * FROM `dtc-392100.dezoomcamp.rides` LIMIT 1000
```

![img_5.png](imgs%2Fimg_5.png)  

Now, run this query to remove all rows.

```
DELETE FROM `dtc-392100.dezoomcamp.rides` WHERE true;
```

You should see T**his statement removed 1,369,765 rows from rides**.  

Back to our code, and add a function to write to BigQuery.  

File `etl_gcs_to_bq.py`

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(log_prints=True, retries=3)
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning"""
    df = pd.read_parquet(path)
    print(f"Pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"Post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True, retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-392100",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    """main ETL flow to load data into Big Query"""
    color='yellow'
    year=2021
    month=1

    path = extract_from_gcs(color,year,month)
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()
```

See [prefect_gcp.credentials](https://prefecthq.github.io/prefect-gcp/credentials/) for more information about handling
GCP credentials.

See also [pandas.DataFrame.to_gbq](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_gbq.html) for more
information about `.to_gbq` method.  

Okay, let's run that program: `python flows/02_gcp/etl_gcs_to_bq.py`  

We should see this result:

```
09:48:48.260 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Finished in state Completed()
09:48:48.284 | INFO    | Flow run 'radical-porpoise' - Created task run 'transform-a7d916b4-0' for task 'transform'
09:48:48.284 | INFO    | Flow run 'radical-porpoise' - Executing 'transform-a7d916b4-0' immediately...
09:48:48.421 | INFO    | Task run 'transform-a7d916b4-0' - Pre: missing passenger count: 98352
09:48:48.425 | INFO    | Task run 'transform-a7d916b4-0' - Post: missing passenger count: 0
09:48:48.447 | INFO    | Task run 'transform-a7d916b4-0' - Finished in state Completed()
09:48:48.468 | INFO    | Flow run 'radical-porpoise' - Created task run 'write_bq-b366772c-0' for task 'write_bq'
09:48:48.469 | INFO    | Flow run 'radical-porpoise' - Executing 'write_bq-b366772c-0' immediately...
09:49:04.306 | INFO    | Task run 'write_bq-b366772c-0' - Finished in state Completed()
09:49:04.332 | INFO    | Flow run 'radical-porpoise' - Finished in state Completed('All states completed.')
```

Now, return to query interface on Google Cloud and run this query.

``` sql
SELECT count(1) FROM `dtc-392100.dezoomcamp.rides`;
```

This should return 1369765.

Now, run this query to remove all rows.

``` sql
DELETE FROM `dtc-392100.dezoomcamp.rides` WHERE true;
```

### 2.6 Parametrizing Flow & Deployments

See [DE Zoomcamp 2.2.5 - Parametrizing Flow & Deployments with ETL into GCS
flow](https://www.youtube.com/watch?v=QrDxPjX10iw) on Youtube.

We will see in this section:

- Parametrizing the script from your flow (rather then hard coded)
- Parameter validation with Pydantic
- Creating a deployment locally
- Setting up Prefect Agent
- Running the flow
- Notifications  

### Parametrizing the script from your flow

Create a new file `parameterized_flow.py` with script parametrized.  

**File `parameterized_flow.py`**

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=3,cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True, retries=3)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True, retries=3)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Wirte DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True, retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
        months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    color = "yellow"
    months = [1,2,3]
    year = 2021
    etl_parent_flow(months, year, color)
```

### Parameter validation with Pydantic

There are many ways to create a deployment, but I will use
[CLI](https://docs.prefect.io/concepts/deployments/#create-a-deployment-on-the-cli)  

See [Deployments overview](https://docs.prefect.io/concepts/deployments/#deployments-overview) for more information.  

Make sure Prefect Orion is running. If not, then run these commands.
```bash
$ prefect orion start
```
Check out the dashboard at <http://127.0.0.1:4200>.

### Creating a deployment locally  

Open a new terminal window and execute this command to create a deployment file.  

```text
$ prefect deployment build ./flows/01_start/parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
```

A deployment model `etl_parent_flow-deployment.yaml` is created.  

### Running the flow  

``` bash
prefect deployment apply etl_parent_flow-deployment.yaml
```

![img_6.png](imgs%2Fimg_6.png)  

Go to the Orion UI. We should see the deployment model is there.  

|                                |                                |
|--------------------------------|--------------------------------|
| ![img_7.png](imgs%2Fimg_7.png) | ![img_8.png](imgs%2Fimg_8.png) |

Click on **Quick run** button.

Select **Flow Runs** in the left menu. Orion UI should indicate that our run is in **Scheduled** state. In my case, I
see **Late** state.

The **Scheduled** state indicates that our flow a ready to be run but we have no agent picking of this run.

Select **Work Queues** in the left menu.

A agent is a very lightly python process that is living in my executing environment.  

![img_9.png](imgs%2Fimg_9.png)  

### Setting up Prefect Agent  

Now start the agent.

``` bash
prefect agent start --work-queue "default"
```

We see this below in the terminal window.

```text
(venv) (base) sang@sang-desktop:~/Desktop/Learning/DE/DTC/week_2$ prefect agent start --work-queue "default"
Starting v2.7.7 agent with ephemeral API...

  ___ ___ ___ ___ ___ ___ _____     _   ___ ___ _  _ _____
 | _ \ _ \ __| __| __/ __|_   _|   /_\ / __| __| \| |_   _|
 |  _/   / _|| _|| _| (__  | |    / _ \ (_ | _|| .` | | |
 |_| |_|_\___|_| |___\___| |_|   /_/ \_\___|___|_|\_| |_|


Agent started! Looking for work from queue(s): default...
10:47:40.156 | INFO    | prefect.agent - Submitting flow run 'c9b81613-01e9-4f0c-be54-6e8912a6542c'
10:47:40.232 | INFO    | prefect.infrastructure.process - Opening process 'meteoric-caribou'...
10:47:40.249 | INFO    | prefect.agent - Completed submission of flow run 'c9b81613-01e9-4f0c-be54-6e8912a6542c'
<frozen runpy>:128: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
10:47:41.907 | INFO    | Flow run 'meteoric-caribou' - Downloading flow code from storage at '/home/sang/Desktop/Learning/DE/DTC/week_2'
10:47:43.826 | INFO    | Flow run 'meteoric-caribou' - Created subflow run 'mottled-mayfly' for flow 'etl-web-to-gcs'
10:47:43.890 | INFO    | Flow run 'mottled-mayfly' - Created task run 'fetch-ba00c645-0' for task 'fetch'
10:47:43.891 | INFO    | Flow run 'mottled-mayfly' - Executing 'fetch-ba00c645-0' immediately...
10:47:43.917 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Cached(type=COMPLETED)
10:47:45.502 | INFO    | Flow run 'mottled-mayfly' - Created task run 'clean-2c6af9f6-0' for task 'clean'
10:47:45.502 | INFO    | Flow run 'mottled-mayfly' - Executing 'clean-2c6af9f6-0' immediately...
10:47:45.913 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID tpep_pickup_datetime  ... total_amount  congestion_surcharge
0       1.0  2021-01-01 00:30:10  ...         11.8                   2.5
1       1.0  2021-01-01 00:51:20  ...          4.3                   0.0

[2 rows x 18 columns]
10:47:45.914 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
10:47:45.914 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 1369765
10:47:45.939 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
10:47:45.957 | INFO    | Flow run 'mottled-mayfly' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
10:47:45.957 | INFO    | Flow run 'mottled-mayfly' - Executing 'write_local-09e9d2b8-0' immediately...
10:47:48.962 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
10:47:48.984 | INFO    | Flow run 'mottled-mayfly' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
10:47:48.985 | INFO    | Flow run 'mottled-mayfly' - Executing 'write_gcs-67f8f48e-0' immediately...
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
10:47:49.601 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
10:47:49.602 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'dtc_data_lake_dtc-392100'.
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
10:47:50.104 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
10:47:52.208 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-01.parquet') to the bucket 'dtc_data_lake_dtc-392100' path 'data/yellow/yellow_tripdata_2021-01.parquet'.
10:47:58.749 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
10:47:58.781 | INFO    | Flow run 'mottled-mayfly' - Finished in state Completed('All states completed.')
10:47:58.853 | INFO    | Flow run 'meteoric-caribou' - Created subflow run 'tough-mongrel' for flow 'etl-web-to-gcs'
10:47:58.919 | INFO    | Flow run 'tough-mongrel' - Created task run 'fetch-ba00c645-0' for task 'fetch'
10:47:58.920 | INFO    | Flow run 'tough-mongrel' - Executing 'fetch-ba00c645-0' immediately...
10:47:58.945 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Cached(type=COMPLETED)
10:48:00.536 | INFO    | Flow run 'tough-mongrel' - Created task run 'clean-2c6af9f6-0' for task 'clean'
10:48:00.537 | INFO    | Flow run 'tough-mongrel' - Executing 'clean-2c6af9f6-0' immediately...
10:48:00.947 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID tpep_pickup_datetime  ... total_amount  congestion_surcharge
0       1.0  2021-02-01 00:40:47  ...         12.3                   2.5
1       1.0  2021-02-01 00:07:44  ...         13.3                   0.0

[2 rows x 18 columns]
10:48:00.948 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
10:48:00.949 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 1371708
10:48:00.970 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
10:48:01.001 | INFO    | Flow run 'tough-mongrel' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
10:48:01.001 | INFO    | Flow run 'tough-mongrel' - Executing 'write_local-09e9d2b8-0' immediately...
10:48:04.001 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
10:48:04.026 | INFO    | Flow run 'tough-mongrel' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
10:48:04.027 | INFO    | Flow run 'tough-mongrel' - Executing 'write_gcs-67f8f48e-0' immediately...
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
10:48:04.584 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
10:48:04.585 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'dtc_data_lake_dtc-392100'.
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
10:48:05.090 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
10:48:07.157 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-02.parquet') to the bucket 'dtc_data_lake_dtc-392100' path 'data/yellow/yellow_tripdata_2021-02.parquet'.
10:48:13.930 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
10:48:13.960 | INFO    | Flow run 'tough-mongrel' - Finished in state Completed('All states completed.')
10:48:14.020 | INFO    | Flow run 'meteoric-caribou' - Created subflow run 'gabby-hog' for flow 'etl-web-to-gcs'
10:48:14.094 | INFO    | Flow run 'gabby-hog' - Created task run 'fetch-ba00c645-0' for task 'fetch'
10:48:14.094 | INFO    | Flow run 'gabby-hog' - Executing 'fetch-ba00c645-0' immediately...
10:48:14.123 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Cached(type=COMPLETED)
10:48:16.275 | INFO    | Flow run 'gabby-hog' - Created task run 'clean-2c6af9f6-0' for task 'clean'
10:48:16.276 | INFO    | Flow run 'gabby-hog' - Executing 'clean-2c6af9f6-0' immediately...
10:48:16.824 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID tpep_pickup_datetime  ... total_amount  congestion_surcharge
0       2.0  2021-03-01 00:22:02  ...          4.3                   0.0
1       2.0  2021-03-01 00:24:48  ...          3.8                   0.0

[2 rows x 18 columns]
10:48:16.825 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
10:48:16.825 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 1925152
10:48:16.847 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
10:48:16.869 | INFO    | Flow run 'gabby-hog' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
10:48:16.870 | INFO    | Flow run 'gabby-hog' - Executing 'write_local-09e9d2b8-0' immediately...
10:48:20.743 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
10:48:20.765 | INFO    | Flow run 'gabby-hog' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
10:48:20.765 | INFO    | Flow run 'gabby-hog' - Executing 'write_gcs-67f8f48e-0' immediately...
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
10:48:21.345 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
10:48:21.345 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'dtc_data_lake_dtc-392100'.
/home/sang/Desktop/Learning/DE/DTC/week_2/venv/lib/python3.11/site-packages/google/auth/_default.py:78: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds. 
  warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
10:48:21.830 | WARNING | google.auth._default - No project ID could be determined. Consider running `gcloud config set project` or setting the GOOGLE_CLOUD_PROJECT environment variable
10:48:22.512 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-03.parquet') to the bucket 'dtc_data_lake_dtc-392100' path 'data/yellow/yellow_tripdata_2021-03.parquet'.
10:48:29.197 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
10:48:29.228 | INFO    | Flow run 'gabby-hog' - Finished in state Completed('All states completed.')
10:48:29.256 | INFO    | Flow run 'meteoric-caribou' - Finished in state Completed('All states completed.')
10:48:30.066 | INFO    | prefect.infrastructure.process - Process 'meteoric-caribou' exited cleanly.
```
And in the Orion UI, we see that the run is completed.  

![img_10.png](imgs%2Fimg_10.png)

### Notifications

We can setup a notification.

Go to the Orion UI, select **Notifications** and create a notification.  

![img_11.png](imgs%2Fimg_11.png)  

Quit the terminal window with `Ctrl+C`.

We should also delete the file created in the bucket.  

![img_12.png](imgs%2Fimg_12.png)


