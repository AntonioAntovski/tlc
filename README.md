# TLC Data Pipeline

This is a implementation of a custom pipeline which uses "The New York City Taxi and Limousine Commission" (TLC) data
(https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Description of the pipeline

The proposed implementation of the pipeline consists of a several steps through which the data is
downloaded, transformed and analysed. The pipeline itself is really simple and easy to modify.

### Config file

I use the `config.py` file as a global config file, where all the global variables,
such as `TRIPDATA_URL` and `TAXI_TYPES`, are defined.

### Data fetcher

The `data_fetcher.py` contains the definition of the `DataFetcher` class. The only method that
this class has is the `fetch_data` method, which downloads the data, for all the defined
`TAXI_TYPES`, for the given year and month.

After downloading, the CSV file is saved into the `dataset` directory, with the corresponding name.

### Data transformer

The `data_transformer.py` contains the definition of the `DataTransformer` class.
The `DataTransformer` provides a couple of methods which are used for transforming the initial data.

### Cron fetcher

The `cron_fetcher.py` contains the definition of the `CronFetcher` class. The main task
of the cron job is to automatically download the new datasets.

If a cron job doesn't already exists, it will create one and run the task. The cron job is
set to run every (first day of each) month.

### Pipeline

The `Pipeline` contains a `DataFetcher` and a `DataTransformer` instance. Running the pipeline
with a given year and month, it would download the data in the corresponding time range,
merge it to one big dataset, transform it to parquet and avro format, and save it into the
`dataset` folder.

If the pipeline is not run with the year and month arguments, it will download the data
from the previous month, i.e. only the most recent datasets.

## Deployment

#### Configuring the project

1. Download or clone this project to your PC
2. Create a virtual environment for the project `python -m venv venv`
3. Activate the virtual environment `source venv/bin/activate`
4. Install the requirements `pip install -r requirements.txt`

#### Running the pipeline

5. `python pipeline.py [--year=2018] [--month=7]`

#### Creating and running the cron job

6. `python cron_fetcher.py`