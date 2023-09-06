
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
import psycopg2


# # PostgreSQL connection details
# host = '172.17.0.3'
# port = '5432'
# database = 'postgres'
# user = 'postgres'
# password = 'mysecretpassword'

# # Connect to the PostgreSQL database using psycopg2
# conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)

# # Create a cursor object to execute SQL statements
# cursor = conn.cursor()


spark = SparkSession.builder \
    .appName("Spark SQL") \
    .config("spark.driver.extraClassPath", "/home/balukarthikram/semusi/postgresql-42.2.6.jar") \
    .getOrCreate()

get_params = {
    'project_directory': '/home/balukarthikram/semusi',
    'database_name': 'postgres',
    'data_gen_path': '/home/balukarthikram/semusi/attribution_data.csv',
    'raw_data_path': '/opt/spark/test/attribution_data.csv',
    'bronze_tbl_path': '/opt/spark/test/bronze',
    'gold_user_journey_tbl_path': '/home/balukarthikram/semusi/gold_user_journey',
    'gold_attribution_tbl_path': '/home/balukarthikram/semusi/gold_attribution',
    'gold_ad_spend_tbl_path': '/home/balukarthikram/semusi/gold_ad_spend'
}

params = get_params
database_name = params['database_name']
raw_data_path = params['raw_data_path']
bronze_tbl_path = params['bronze_tbl_path']



# Define the schema
schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("conversion", IntegerType(), True),
    StructField("ad_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("event_type", StringType(), True)
])

# Read the streaming data from the raw data path
raw_data_df = spark.readStream.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(raw_data_path)

# Perform transformations on the data
transformed_data_df = raw_data_df.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("conversion", col("conversion").cast("int"))


# Write the data to PostgreSQL
stream_query = transformed_data_df.writeStream \
    .foreachBatch(lambda batch, batchId: batch.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.17.0.3:5432/postgres") \
        .option("dbtable", "bronze") \
        .option("user", "postgres") \
        .option("password", "mysecretpassword") \
        .mode("append") \
        .save()) \
    .option("checkpointLocation", bronze_tbl_path + "/checkpoint") \
    .start()




# Delete the old database and tables if needed
spark.sql('DROP DATABASE IF EXISTS {} CASCADE'.format(database_name))

# Create database to house tables
spark.sql('CREATE DATABASE {}'.format(database_name))

# Create bronze table
spark.sql('''
    CREATE TABLE {}.bronze
    (time timestamp, conversion int)
    USING jdbc
    OPTIONS (
        url 'jdbc:postgresql://172.17.0.3:5432/postgres',
        dbtable 'bronze',
        user 'postgres',
        password 'mysecretpassword'
    )
    '''.format(database_name))


# Read the data from the bronze table
bronze_tbl = spark.table("{}.bronze".format(database_name))

bronze_tbl.show()





