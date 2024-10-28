import os 
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col

import util.config as conf
from util.logger import Log4j

spark_conf = conf.get_spark_conf()
spark = SparkSession.builder \
        .config(conf = spark_conf) \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
log = Log4j(spark)
summary_df = spark.read.parquet("/data/window-function/summary.parquet")
summary_df.show()

country_weekend_window = Window.partitionBy("Country").orderBy(col("InvoiceValue").desc())

df_rank =  summary_df \
            .withColumn('rank',f.dense_rank().over(country_weekend_window)) 

df_rank.show()

country_window = Window.partitionBy("Country").orderBy("WeekNumber")

current_value = col("InvoiceValue")
previous_value = f.lag(col("InvoiceValue")).over(country_window)

week_df = summary_df \
        .withColumn('TotalValue',f.sum(col("InvoiceValue")).over(country_window)) \
        .withColumn('percentage compared to the previous value',
                    f.round(f.when(previous_value.isNull(),0.0)
                    .otherwise((current_value-previous_value)/previous_value*100),2))
week_df.show()