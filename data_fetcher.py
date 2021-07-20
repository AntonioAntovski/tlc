import urllib.request
import config
import os
from loguru import logger


class DataFetcher:
    def __init__(self):
        self.url = config.TRIPDATA_URL
        self.taxi_types = config.TAXI_TYPES

    def fetch_data(self, year, month):
        """
        Fetch NYC trip data (for all types of taxis) for the given year and month, and save it in the dataset folder
        :param year:
        :param month:
        :return:
        """

        month = f"0{month}" if month < 10 else f"{month}"

        if not os.path.exists("dataset"):
            os.mkdir("dataset")

        for type in self.taxi_types:
            logger.info(
                f"Fetching https://s3.amazonaws.com/nyc-tlc/trip+data/{type}_tripdata_{year}-{month}.csv..."
            )

            try:
                urllib.request.urlretrieve(
                    f"https://s3.amazonaws.com/nyc-tlc/trip+data/{type}_tripdata_{year}-{month}.csv",
                    f"dataset/{type}_tripdata_{year}-{month}.csv",
                )
            except Exception as e:
                logger.error(
                    f"Fetching https://s3.amazonaws.com/nyc-tlc/trip+data/{type}_tripdata_{year}-{month}.csv failed\n{str(e)}"
                )
                continue
