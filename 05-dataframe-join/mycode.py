import os
from pyspark.sql import SparkSession
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
# Thêm cấu hình legacy để hỗ trợ định dạng cũ
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
flight_time_df1 = spark.read.json("/data/dataframe-join/d1/")
flight_time_df2 = spark.read.json("/data/dataframe-join/d2/")
flight_time_df1 = flight_time_df1 \
                .withColumn('year', f.year(f.to_date(col("FL_DATE"), 'M/d/yyyy'))) \
                .withColumn('date',f.to_date(col("FL_DATE"), 'M/d/yyyy'))
join_df = flight_time_df1.join(flight_time_df2,"id","inner")

cancle_df = join_df \
            .filter((col('year')==2000)&
                    (col("DEST_CITY_NAME")=="Atlanta, GA")&
                    (col("CANCELLED")=="1")) \
            .orderBy(col("date").desc())
print("cancled flight to Alanta, GA in 2000")
cancle_df\
    .select(
        col('id'),
        col("DEST"),
        col("DEST_CITY_NAME"),
        col("ORIGIN"),
        col("ORIGIN_CITY_NAME"),
        col("DISTANCE"),
        col("date"),
        col("OP_CARRIER"),
        col("CRS_ARR_TIME"),
        col("CRS_DEP_TIME"),
    ) \
    .show()
print(" lấy ra danh sách các destination, năm và tổng số chuyến bay bị hủy của năm đó.")
cancle2_df = join_df \
            .filter(col("CANCELLED")=="1") \
            .groupBy("DEST_CITY_NAME","year") \
            .agg(
                f.count("*").alias("NUM_CANCELLED_FLIGHT")
            ) \
            .orderBy(col("DEST_CITY_NAME"),col("year"))
cancle2_df.show()