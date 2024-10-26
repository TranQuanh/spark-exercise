import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
import util.config as conf
from util.logger import Log4j


working_dir = os.getcwd()
spark_conf = conf.get_spark_conf()
spark = SparkSession \
        .builder \
        .config(conf = spark_conf) \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
log = Log4j(spark)

survey_df = spark.read \
            .option("header","true") \
            .option("inferSchema","true") \
            .csv(path="/data/dataframe-api/survey.csv")
log.info("survey_df schema : ")

count_df = survey_df \
        .filter((col('Age') < 40) & ((f.lower(col('Gender')) == 'male') | (f.lower(col('Gender')) == 'm'))) \
        .groupBy("Country") \
        .agg(f.count("*").alias("count")) \
        .orderBy(col("count").desc(),col("Country"))
log.info("count_df:" )
count_df.show()

genderDF = survey_df \
        .select(col('Gender'),
                col('Country'),
                f.when((f.lower(col('Gender')) =='male') | (f.lower(col('Gender'))=='m'),1)
                .otherwise(0).alias('num_male'),
                f.when((f.lower(col('Gender')) == 'female') | (f.lower(col('Gender')) =='f'),1)
                .otherwise(0).alias('num_female')
        )

genderDF.show()

aggDF = genderDF \
        .groupBy('Country') \
        .agg(
                f.sum('num_male').alias('num_male'),
                f.sum('num_female').alias('num_female') ) \
        .orderBy('Country')
aggDF.show(100)
spark.stop()