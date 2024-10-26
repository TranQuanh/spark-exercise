from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from datetime import date
from pyspark.sql import Row

data = [
  Row(id = 1, value = 28.3, date = date(2021,1,1)),
  Row(id = 2, value = 15.8, date = date(2021,1,1)),
  Row(id = 3, value = 20.1, date = date(2021,1,2)),
  Row(id = 4, value = 12.6, date = date(2021,1,3))
]

df = spark.createDataFrame(data)

print(type(df))
