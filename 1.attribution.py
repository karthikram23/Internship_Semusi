from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
import time
import psycopg2





spark = SparkSession \
    .builder \
    .appName("Spark SQL") \
    .config("spark.jars", "./postgresql-42.2.6.jar") \
    .getOrCreate()

# Configure PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://172.17.0.3:5432/"
connection_properties = {
    "user": "appice",
    "password": "Appicefish123@",
    "driver": "org.postgresql.Driver"
}

get_params = {
     'project_directory': '/opt/spark/test',
    'database_name': 'appice',
    'data_gen_path': '/opt/spark/test',
    'raw_data_path': '/opt/spark/test/raw',
    'bronze_tbl_path': '/opt/spark/test/bronze',
    'gold_user_journey_tbl_path': '/opt/spark/test/gold_user_journey',
    'gold_attribution_tbl_path': '/opt/spark/test/gold_attribution',
    'gold_ad_spend_tbl_path': '/opt/spark/test/gold_ad_spend'
}

params = get_params
database_name = params['database_name']
raw_data_path = params['raw_data_path']
bronze_tbl_path = params['bronze_tbl_path']

schema = spark.read.csv(raw_data_path,header=True).schema

raw_data_df = spark.readStream.format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(raw_data_path)

raw_data_df = raw_data_df.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("conversion", col("conversion").cast("integer"))

# Write the streaming data to PostgreSQL table
def process_row(raw_data_df, epoch_id):
    raw_data_df.write.jdbc(url=jdbc_url, table="bronze", mode="append", properties=connection_properties)
    pass
query = raw_data_df.writeStream.foreachBatch(process_row).start() \
    # .trigger(once=True)
# query = raw_data_df.writeStream.format("jdbc") \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://172.17.0.3:5432/") \
#         .option("dbtable", "bronze") \
#         .option("user", "appice") \
#         .option("password", "Appicefish123@") \
#         .mode("append") \
#         .save()
#     ) \
#     .trigger(once=True) \
#     .start()
# query.awaitTermination()


# Sleep for a while to allow the stream to initialize
time.sleep(30)
