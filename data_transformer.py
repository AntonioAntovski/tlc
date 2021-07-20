import os
import pandas as pd
import pandavro as pdx
from loguru import logger


class DataTransformer:
    @staticmethod
    def read_csv(csv_filename):
        """
        Read a CSV file
        :param csv_filename: Path to the CSV file
        :return: DataFrame object
        """

        df = pd.read_csv(csv_filename)
        return df

    @staticmethod
    def generate_avro_schema(df_dtypes):
        """
        Generate Avro schema
        :param df_dtypes: DataFrame column types
        :return: Avro schema (JSON representation)
        """

        schema = {
            "namespace": "tlc.avro",
            "type": "record",
            "name": "tlc",
            "fields": [
                {
                    "name": key,
                    "type": [
                        "null",
                        df_dtypes.get(key)
                        .name.lower()
                        .replace("int64", "long")
                        .replace("float64", "double")
                        .replace("object", "string"),
                    ],
                }
                for key in df_dtypes.keys()
            ],
        }

        return schema

    def merge_yellow_green_datasets(self, parent_folder):
        """
        Transform and merge all of the yellow / green datasets into one big dataset. The resulting dataset is saved
        into the parent_folder.
        :param parent_folder: Path to the folder containing the dataset files
        :return:
        """

        merged = pd.DataFrame()
        green_data = [
            file
            for file in os.listdir(parent_folder)
            if "green" in file and ".csv" in file
        ]

        for filename in green_data:
            date = filename.split("_")[-1].replace(".csv", "")
            green_df = self.read_csv(f"{parent_folder}/{filename}")
            yellow_df = self.read_csv(
                f"{parent_folder}/{filename.replace('green', 'yellow')}"
            )

            # Here we drop the 'ehail_fee' column which contains only null values
            green_df = green_df.dropna(axis=1, how="all")

            # Rename the pickup/dropoff_datetime columns
            green_df = green_df.rename(
                columns={
                    "lpep_pickup_datetime": "pickup_datetime",
                    "lpep_dropoff_datetime": "dropoff_datetime",
                }
            )
            yellow_df = yellow_df.rename(
                columns={
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                }
            )

            # Add the trip_type column with a (dummy) value 0
            yellow_df["trip_type"] = 0

            merged = merged.append(yellow_df.append(green_df))

        # Cast the datetime (string) columns to datetime objects
        merged.pickup_datetime = pd.to_datetime(merged.pickup_datetime, errors="coerce")
        merged.dropoff_datetime = pd.to_datetime(
            merged.dropoff_datetime, errors="coerce"
        )

        # Add day_of_week column
        merged["day_of_week_pickup"] = merged.pickup_datetime.dt.day_name()
        merged["day_of_week_dropoff"] = merged.dropoff_datetime.dt.day_name()

        # Add hour columns
        merged["hour_pickup"] = merged.pickup_datetime.dt.hour
        merged["hour_dropoff"] = merged.dropoff_datetime.dt.hour

        merged.to_csv(f"{parent_folder}/yellow_green_tripdata.csv", index=False)

    def to_parquet_format(self, csv_file_name):
        """
        Convert CSV to Parquet format. The Parquet file is saved in the dataset folder
        :param csv_file_name: Path to the CSV file
        :return:
        """

        logger.info(f"{csv_file_name} to parquet format...")
        df = self.read_csv(csv_file_name)
        df.to_parquet(csv_file_name.replace(".csv", ".parquet"))

    def to_avro_format(self, csv_file_name):
        """
        Convert CSV to Avro format. The Avro file is saved in the dataset folder
        :param csv_file_name: Path to the CSV file
        :return:
        """

        logger.info(f"{csv_file_name} to avro format...")
        df = self.read_csv(csv_file_name)

        # Cast the store_and_fwd_flag to string (nan causes ValueError exception)
        try:
            df.store_and_fwd_flag = df.store_and_fwd_flag.astype(str)
        except:
            pass

        pdx.to_avro(
            csv_file_name.replace(".csv", ".avro"),
            df,
            schema=self.generate_avro_schema(df.dtypes),
        )
