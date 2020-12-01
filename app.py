from pyspark.sql import SparkSession
from transformations import Transformations
from file import File
from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder \
                    .master('local') \
                    .appName('learning-spark') \
                    .config('spark.executor.memory', '8gb') \
                    .getOrCreate()
sc = spark.sparkContext

df = File(spark).load_file(str("CaliforniaHousing/cal_housing.data"))

# Transformação e padronização do dataframe
transformation = Transformations(spark)
df = df.transform(transformation.normalize_medianHouseValue) \
        .transform(transformation.add_roomsPerHousehold) \
        .transform(transformation.add_populationPerHousehold) \
        .transform(transformation.add_bedroomsPerRoom) \
        .transform(transformation.reorder_columns) \
        .transform(transformation.standarization)

# Pré processamento de dados
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
scaler = standardScaler.fit(df)
scaled_df = scaler.transform(df)
train_data, test_data = scaled_df.randomSplit([.8,.2],seed=1234)

# Treinamento e predição
reg = LinearRegression(labelCol='label', maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = reg.fit(train_data)
predicted = model.predict(test_data)

# Validação
print(model.summary.rootMeanSquaredError)
print(model.summary.r2)