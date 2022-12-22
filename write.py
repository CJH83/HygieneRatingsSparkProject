from pyspark.sql import SparkSession

class WriteData:
    """
    A class to write a spark dataframe to a csv file.

    Attributes:
    spark: SparKSession
        Holds the spark session
    """

    def __init__(self, spark):
        self.spark = spark

    def write_data_to_csv(self, dataframe):
        """
        Writes a spark dataframe to a csv to HDFS

        :param dataframe:
        """
        try:
            dataframe.write.csv('Total_Hygiene_Ratings_By_Local_Authority')
        except Exception as error:
            print(error)
            raise Exception('HDFS directory already exists')

