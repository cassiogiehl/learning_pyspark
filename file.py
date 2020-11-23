from pyspark.sql import SparkSession, DataFrame, types, functions
from pyspark.sql.types import *
from schema import Schema

class File:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def load_file(self, filename: str, delimiter: str = ",")-> DataFrame:
        return self._spark.read \
                        .option("delimiter", delimiter) \
                        .csv(filename, schema=Schema.get_schema(), inferSchema=False)
