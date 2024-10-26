import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

import util.config as conf
from util.logger import Log4j


def parse_gender(gender):
    male_pattern = r"^m$|^male$|^man$"
    female_pattern = r"^f$|^female$|^woman$"
    if re.search(male_pattern, gender.lower()):
        return "Male"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    return "Unknown"


if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    log = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/data/udf/survey.csv")

    log.info("survey_df schema:")
    survey_df.printSchema()

    log.info("survey_df:")
    survey_df.show()

    parse_gender_udf = udf(parse_gender, returnType=StringType())
    log.info("Catalog Entry:")
    for r in spark.catalog.listFunctions():
        if "parse_gender" in r.name:
            log.info(r)

    survey_df.withColumn("Gender", parse_gender_udf("Gender")) \
        .select("Age", "Gender", "Country", "state", "no_employees") \
        .show()

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    log.info("Catalog Entry:")
    for r in spark.catalog.listFunctions():
        if "parse_gender" in r.name:
            log.info(r)

    survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)")) \
        .select("Age", "Gender", "Country", "state", "no_employees") \
        .show()

    spark.stop()
