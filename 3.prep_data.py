# import matplotlib.pyplot as plt
# import seaborn as sns
# from pyspark.sql import SparkSession
# from pyspark.sql.utils import AnalysisException
# import psycopg2

# # Define the parameters
# get_params = {
#     'project_directory': '/home/balukarthikram/semusi',
#     'database_name': 'postgres',
#     'data_gen_path': '/home/balukarthikram/semusi/attribution_data.csv',
#     'raw_data_path': '/home/balukarthikram/semusi/raw/attribution_data.csv',
#     'bronze_tbl_path': '/home/balukarthikram/semusi/bronze',
#     'gold_user_journey_tbl_path': '/home/balukarthikram/semusi/gold_user_journey',
#     'gold_attribution_tbl_path': '/home/balukarthikram/semusi/gold_attribution',
#     'gold_ad_spend_tbl_path': '/home/balukarthikram/semusi/gold_ad_spend',
# }

# # Create a SparkSession
# spark = SparkSession.builder \
#     .appName("Spark SQL") \
#     .getOrCreate()

# params = get_params
# database_name = params['database_name']
# gold_user_journey_tbl_path = params['gold_user_journey_tbl_path']
# gold_attribution_tbl_path = params['gold_attribution_tbl_path']




# db_host = '172.17.0.3'
# db_port = '5432'
# db_user = 'localhost'
# db_password = 'mysecretpassword'
# database_name = 'postgres'

# # Establish a connection to the database
# try:
#     connection = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database_name)
#     # Rest of the code
# except psycopg2.OperationalError as e:
#     print("Error connecting to the PostgreSQL server:", str(e))


# try:
#     spark.sql("USE {}".format(database_name))
# except AnalysisException as e:
#     print("Error: ", str(e))

# # Create or replace the user_journey_view
# spark.sql('''
#     CREATE OR REPLACE TEMP VIEW user_journey_view AS
#     SELECT
#         sub2.uid AS uid,
#         CASE WHEN sub2.conversion = 1 THEN concat('Start > ', sub2.path, ' > Conversion')
#              ELSE concat('Start > ', sub2.path, ' > Null') END AS path,
#         sub2.first_interaction AS first_interaction,
#         sub2.last_interaction AS last_interaction,
#         sub2.conversion AS conversion,
#         sub2.visiting_order AS visiting_order
#     FROM (
#         SELECT
#             sub.uid AS uid,
#             concat_ws(' > ', collect_list(sub.channel)) AS path,
#             element_at(collect_list(sub.channel), 1) AS first_interaction,
#             element_at(collect_list(sub.channel), -1) AS last_interactioan,
#             element_at(collect_list(sub.conversion), -1) AS conversion,
#             collect_list(sub.visit_order) AS visiting_order
#         FROM (
#             SELECT
#                 uid,
#                 channel,
#                 time,
#                 conversion,
#                 dense_rank() OVER (
#                     PARTITION BY uid
#                     ORDER BY time ASC
#                 ) AS visit_order
#             FROM
#                 bronze
#         ) AS sub
#         GROUP BY sub.uid
#     ) AS sub2
# ''')

# # Query the user_journey_view
# user_journey_df = spark.sql('SELECT * FROM user_journey_view')

# # Show the data in the user_journey_df DataFrame
# user_journey_df.show()

# # Create or replace the gold_user_journey table
# spark.sql('''
#     CREATE TABLE IF NOT EXISTS '{}' .gold_user_journey
#     USING DELTA
#     LOCATION '{}'
#     AS SELECT * FROM user_journey_view
# '''.format(params['gold_user_journey_tbl_path']))

# # Optimize the gold_user_journey table with ZORDER by uid
# spark.sql('OPTIMIZE gold_user_journey ZORDER BY uid')

# # Query the gold_user_journey table
# spark.sql("SELECT * FROM gold_user_journey").show()


# # Create or replace the attribution_view
# spark.sql('''
#     CREATE OR REPLACE TEMP VIEW attribution_view AS
#     SELECT
#         'first_touch' AS attribution_model,
#         first_interaction AS channel,
#         ROUND(COUNT(*) / (
#             SELECT COUNT(*)
#             FROM gold_user_journey
#             WHERE conversion = 1),2) AS attribution_percent
#         FROM gold_user_journey
#         WHERE conversion = 1
#         GROUP BY first_interaction
#         UNION
#         SELECT
#         'last_touch' AS attribution_model,
#         last_interaction AS channel,
#         round(count(*) /(
#             SELECT COUNT(*)
#             FROM gold_user_journey
#             WHERE conversion = 1),2) AS attribution_percent
#         FROM gold_user_journey
#         WHERE conversion = 1
#         GROUP BY last_interaction
# ''')
          
# spark.sql('''
# CREATE TABLE IF NOT EXISTS gold_attribution
# USING DELTA
# LOCATION '{}'
# AS
# SELECT * FROM attribution_view'''.format(gold_attribution_tbl_path))

# spark.sql("SELECT * FROM gold_attribution").show()


# attribution_pd = spark.table('gold_attribution').toPandas()

# sns.set(font_scale=1.1)
# sns.catplot(x='channel', y='attribution_percent', hue='attribution_model', data=attribution_pd, kind='bar', aspect=2).set_xticklabels(rotation=15)


# # Show the plot
# plt.show()

# spark.sql('''
#     CREATE OR REPLACE TEMP VIEW attribution_view AS
#     SELECT 'first_touch' AS attribution_model, first_interaction AS channel,
#         round(count(*)/(SELECT COUNT(*) FROM gold_user_journey WHERE conversion = 1), 2) AS attribution_percent
#     FROM gold_user_journey
#     WHERE conversion = 1
#     GROUP BY first_interaction
#     UNION
#     SELECT 'last_touch' AS attribution_model, last_interaction AS channel,
#         round(count(*)/(SELECT COUNT(*) FROM gold_user_journey WHERE conversion = 1), 2) AS attribution_percent
#     FROM gold_user_journey
#     WHERE conversion = 1
#     GROUP BY last_interaction
# ''')

# # Query the attribution_view
# attribution_df = spark.sql('SELECT * FROM attribution_view')

# # Show the data in the attribution_df DataFrame
# attribution_df.show()


# spark.sql('''
#     MERGE INTO gold_attribution
#     USING attribution_view
#     ON attribution_view.attribution_model = gold_attribution.attribution_model
#     AND attribution_view.channel = gold_attribution.channel
#     WHEN MATCHED THEN
#         UPDATE SET *
#     WHEN NOT MATCHED
#         THEN INSERT *
# ''')
          

# spark.sql("DESCRIBE gold_user_journey").show()



import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import psycopg2

# Define the parameters
get_params = {
    'project_directory': '/home/balukarthikram/semusi',
    'database_name': 'postgres',
    'data_gen_path': '/home/balukarthikram/semusi/attribution_data.csv',
    'raw_data_path': '/home/balukarthikram/semusi/raw/attribution_data.csv',
    'bronze_tbl_path': '/opt/spark/test/bronze',
    'gold_user_journey_tbl_path': '/opt/spark/test/gold_user_journey',  # Update the path to the Docker volume path
    'gold_attribution_tbl_path': '/opt/spark/test/gold_attribution',  # Update the path to the Docker volume path
    'gold_ad_spend_tbl_path': '/home/balukarthikram/semusi/gold_ad_spend',
}

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Spark SQL") \
    .getOrCreate()

params = get_params
database_name = params['database_name']
gold_user_journey_tbl_path = params['gold_user_journey_tbl_path']
gold_attribution_tbl_path = params['gold_attribution_tbl_path']

db_host = '172.17.0.3'
db_port = '5432'
db_user = 'postgres'
db_password = 'mysecretpassword'
database_name = 'postgres'

# Establish a connection to the database
try:
    connection = psycopg2.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database_name)
    # Rest of the code
except psycopg2.OperationalError as e:
    print("Error connecting to the PostgreSQL server:", str(e))


try:
    spark.sql("USE {}".format(database_name))
except AnalysisException as e:
    print("Error: ", str(e))

# Create or replace the user_journey_view
spark.sql('''
    CREATE OR REPLACE TEMP VIEW user_journey_view AS
    SELECT
        sub2.uid AS uid,
        CASE WHEN sub2.conversion = 1 THEN concat('Start > ', sub2.path, ' > Conversion')
             ELSE concat('Start > ', sub2.path, ' > Null') END AS path,
        sub2.first_interaction AS first_interaction,
        sub2.last_interaction AS last_interaction,
        sub2.conversion AS conversion,
        sub2.visiting_order AS visiting_order
    FROM (
        SELECT
            sub.uid AS uid,
            concat_ws(' > ', collect_list(sub.channel)) AS path,
            element_at(collect_list(sub.channel), 1) AS first_interaction,
            element_at(collect_list(sub.channel), -1) AS last_interactioan,
            element_at(collect_list(sub.conversion), -1) AS conversion,
            collect_list(sub.visit_order) AS visiting_order
        FROM (
            SELECT
                uid,
                channel,
                time,
                conversion,
                dense_rank() OVER (
                    PARTITION BY uid
                    ORDER BY time ASC
                ) AS visit_order
            FROM
                bronze
        ) AS sub
        GROUP BY sub.uid
    ) AS sub2
''')

# Query the user_journey_view
user_journey_df = spark.sql('SELECT * FROM user_journey_view')

# Show the data in the user_journey_df DataFrame
user_journey_df.show()

# Create or replace the gold_user
