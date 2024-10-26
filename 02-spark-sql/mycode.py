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

surveyDF = spark.read \
        .option("header","true") \
        .option("inferschema","true") \
        .csv(path = "/data/spark-sql/survey.csv")
log.info("survet df schema: {}".format(surveyDF))
surveyDF.show(5)
surveyDF.printSchema()
surveyDF.createOrReplaceTempView("survey_view")
countDf = spark.sql(
    """
    SELECT Country, count(*) as count
    FROM survey_view
    WHERE Age < 40
    AND (LOWER(Gender) = 'male' OR LOWER(Gender) = 'm')
    GROUP BY Country
    ORDER BY count desc, Country;
    """
)
log.info("countDf: ")
countDf.show(100)
genderDF = surveyDF \
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

countsqlDF = spark.sql(
    """
    SELECT  
        Country,
        SUM(CASE 
                WHEN LOWER(Gender) = 'male' OR LOWER(Gender) = 'm' THEN 1
                ELSE 0
            END) AS num_male,
        SUM(CASE 
                WHEN LOWER(Gender) = 'female' OR LOWER(Gender) = 'f' THEN 1
                ELSE 0
            END) AS num_female
    FROM survey_view
    GROUP BY Country
    ORDER BY Country;
    """
)
countsqlDF.show()