import argparse
from datetime import datetime
from loguru import logger
from data_fetcher import DataFetcher
from data_transformer import DataTransformer


class Pipeline:
    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.data_transformer = DataTransformer()

    def fetch(self, year, month):
        """
        Fetch all of the datasets from the given year and month until now.
        :param year: The starting year from which we want to fetch the data
        :param month: The starting month from which we want to fetch the data
        :return:
        """

        current_year = datetime.now().year
        current_month = datetime.now().month

        from_month = month
        to_month = 12

        for y in range(year, current_year + 1):
            if y != year:
                from_month = 1

            if y == current_year:
                to_month = current_month

            for m in range(from_month, to_month + 1):
                self.data_fetcher.fetch_data(year=y, month=m)

        logger.success("Successfully fetched the data!")

    def transform(self):
        """
        Merge all of the datasets into one big dataset and transform it to parquet/avro format.
        :return:
        """

        self.data_transformer.merge_yellow_green_datasets("dataset")

        self.data_transformer.to_parquet_format(f"dataset/yellow_green_tripdata.csv")
        self.data_transformer.to_parquet_format(f"dataset/yellow_green_tripdata.csv")

    def run(self, year=None, month=None):
        """
        Run the pipeline.
        :param year: The starting year from which we want to fetch the data
        :param month: The starting month from which we want to fetch the data
        :return:
        """

        self.fetch(
            year=year or datetime.now().year, month=month or datetime.now().month
        )
        self.transform()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run TLC pipeline")
    parser.add_argument(
        "--year",
        help="Fetch TLC data from <year> to now",
        default=datetime.now().year - 1,
    )
    parser.add_argument(
        "--month",
        help="Fetch TLC data from <month> to now",
        default=datetime.now().month - 1,
    )
    args = parser.parse_args()

    pipeline = Pipeline()
    pipeline.run(year=int(args.year), month=int(args.month))
