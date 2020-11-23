from pyspark.sql.types import *

class Schema:

    def get_schema()-> StructType:
        return StructType([
            StructField('longitude', StringType(), True),
            StructField('latitude', StringType(), True),
            StructField('housingMedianAge', FloatType(), True),
            StructField('totalRooms', FloatType(), True),
            StructField('totalBedrooms', FloatType(), True),
            StructField('population', FloatType(), True),
            StructField('households', FloatType(), True),
            StructField('medianIncome', DoubleType(), True),
            StructField('medianHouseValue', FloatType(), True)
        ])

    def get_schema_standarization()-> StructType:
        return StructType([
            StructField('label', FloatType(), True),
            StructField('features', StructType([
                StructField('totalBedrooms', FloatType(), True),
                StructField('population', FloatType(), True),
                StructField('households', FloatType(), True),
                StructField('medianIncome', DoubleType(), True),
                StructField('roomsPerHousehold', DoubleType(), True),
                StructField('populationPerHousehold', DoubleType(), True),
                StructField('bedroomsPerRoom', DoubleType(), True),
            ]))
        ])