from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.ml.linalg import DenseVector
from schema import Schema

class Transformations:
    def __init__(self, spark):
        self._spark = spark
    
    def normalize_medianHouseValue(self, df: DataFrame)-> DataFrame:
        return df.withColumn('medianHouseValue', col('medianHouseValue')/100000)

    def add_roomsPerHousehold(self, df: DataFrame)-> DataFrame:
        return df.withColumn("roomsPerHousehold", col("totalRooms")/col("households"))   

    def add_populationPerHousehold(self, df: DataFrame)-> DataFrame:
        return df.withColumn("populationPerHousehold", col("population")/col("households"))

    def add_bedroomsPerRoom(self, df: DataFrame)-> DataFrame:
        return df.withColumn("bedroomsPerRoom", col("totalBedRooms")/col("totalRooms"))

    def reorder_columns(self, df: DataFrame)-> DataFrame:
        return df.select("medianHouseValue", 
                "totalBedRooms", 
                "population", 
                "households", 
                "medianIncome", 
                "roomsPerHousehold", 
                "populationPerHousehold", 
                "bedroomsPerRoom")
                
    def standarization(self, df: DataFrame) -> DataFrame:
        input_data = df.rdd.map(lambda x: (x[0], DenseVector(x[1:])))
        df = self._spark.createDataFrame(input_data, ['label', 'features'])
        # df = input_data.toDF("label", "features")
        return df