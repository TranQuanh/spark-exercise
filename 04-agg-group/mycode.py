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
invoice_df = spark.read \
            .option("header","true") \
            .option("inferSchema", "true") \
            .csv(path="/data/agg-group/invoices.csv")
log.info("invoice_df schema : " )

invoice_df = invoice_df\
        .withColumn('new_date_string', f.substr(col("InvoiceDate"), f.lit(1), f.lit(10)))

invoice_df = invoice_df \
        .withColumn('year',f.year(f.to_date(col("new_date_string"), "dd-MM-yyyy")))
invoice_df.show()
count_df = invoice_df \
        .groupBy(col("Country"),col("year")) \
        .agg(
            f.count("*").alias("num_invoices"),
            f.sum("Quantity").alias("total_quantity"),
            f.sum("UnitPrice").alias("invoice_value")
        )
customer_df = invoice_df \
        .filter(col('year')== 2010) \
        .groupBy('CustomerID') \
        .agg(
            f.sum(col("Quantity")*col("UnitPrice")).alias("invoice_value")
        )\
        .orderBy(col("invoice_value").desc(),col("CustomerId").asc())
count_df.show(100)
customer_df.show(100)