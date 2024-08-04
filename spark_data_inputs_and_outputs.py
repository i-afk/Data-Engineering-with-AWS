import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

# print details of connection to (sim) cluster
print(
    spark.sparkContext.getConf().getAll()
)

# Path residing in workspace, not HDFS
path = "/workspace/home/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
user_log_df = spark.read.json(path)

# Spark inferres Schema from json file
user_log_df.printSchema()

print(
    user_log_df.describe()
)

user_log_df.show(n=1)
print(
    user_log_df.take(5)
)


# Changing File formats
# Out path
out_path = "/workspace/home/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/exercises/data/sparkify_log_small.csv"

user_log_df.write.mode('overwrite').save(out_path, format = 'csv', header =True)

# creating new dataframe
user_log_2_df = spark.read.csv(out_path, header=True)
user_log_2_df.printSchema()

# choose 2 records from csv file
user_log_2_df.select('userID').show()

print(
    user_log_2_df.take(1)
)