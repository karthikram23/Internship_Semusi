from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("CSV Display") \
    .config("spark.jars", "postgresql-42.2.6.jar") \
    .getOrCreate()

df = spark.read \
    .format("csv") \
    .option('header', 'true') \
    .option("url", "jdbc:postgresql://172.17.0.2:5432/postgres?client_encoding=utf8") \
    .option("dbtable", "syntheticdata") \
    .option("user", "postgres") \
    .option("password", "mysecretpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
df.show('/home/balukarthikram/semusi/raw')





