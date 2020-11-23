from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import LinearRegression

class Model:
    def __init__(self, spark, df):
        self._spark = spark
        self._df = df
        self._df_train = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD)
        self._df_test = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD)

    def fit_transform(self):
        return self.fit().transform()

    def fit(self):
        standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
        scaler = standardScaler.fit(self._df)
        return scaler

    def transform(self):
        scaled_df = scaler.transform(self._df)
        return scaled_df

    def train_test_split(self):
        self._df_train, self._df_test self._df.randomSplit([.8,.2],seed=1234)
        return self._df_train, self._df_test

class Regression():
    def __init__(self):
        self._df_train
        # self._df_test
    
    def linear_regression(self):
        model = LinearRegression(labelCol='label', maxIter=10, regParam=0.3, elasticNetParam=0.8)
        linear_model = model.fit(self._df_train)