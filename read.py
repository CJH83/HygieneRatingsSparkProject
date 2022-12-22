from pyspark.sql import SparkSession


class ReadData:
    """
    A class to read data from json files using spark.

    Attributes:
    spark: SparkSession
        Holds the spark session

    Methods:
    read_data()
        Reads the data from json files using spark
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_data(self):
        """
        Uses Spark to Read two json files into two dataframes

        :return:
            dataframe
            dataframe
        """
        try:
            df1 = self.spark.read.json('sthelens_data.json')
            df2 = self.spark.read.json('wigan_data.json')
        except FileNotFoundError as fnf_error:
            raise Exception(fnf_error)
        return df1, df2


