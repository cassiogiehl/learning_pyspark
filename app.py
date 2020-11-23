from pyspark.sql import SparkSession
from transformations import Transformations
from file import File

spark = SparkSession.builder \
                    .master('local') \
                    .appName('learning-spark') \
                    .config('spark.executor.memory', '8gb') \
                    .getOrCreate()
sc = spark.sparkContext

df = File(spark).load_file(str("CaliforniaHousing/cal_housing.data"))

transformation = Transformations(spark)
df = df.transform(transformation.normalize_medianHouseValue) \
        .transform(transformation.add_roomsPerHousehold) \
        .transform(transformation.add_populationPerHousehold) \
        .transform(transformation.add_bedroomsPerRoom) \
        .transform(transformation.reorder_columns) \
        .transform(transformation.standarization)

df.show()

