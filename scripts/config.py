from pyspark.sql import SparkSession

def create_spark_session(app_name="PySparkPipeline"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    return spark