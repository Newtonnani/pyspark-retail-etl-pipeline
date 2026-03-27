from config import create_spark_session
from logger_setup import get_logger
from pyspark.sql.functions import trim, upper, col

logger = get_logger(__name__)
logger.info("Bronze layer started")

spark = create_spark_session("BronzeLayer")

customers_df = spark.read.csv("data/raw/customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("data/raw/orders.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv("data/raw/order_items.csv", header=True, inferSchema=True)

customers_df = customers_df \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("city", upper(col("city"))) \
    .withColumn("state", upper(col("state")))

orders_df = orders_df.withColumn("status", upper(col("status")))

customers_df.write.mode("overwrite").parquet("data/bronze/customers")
products_df.write.mode("overwrite").parquet("data/bronze/products")
orders_df.write.mode("overwrite").parquet("data/bronze/orders")
order_items_df.write.mode("overwrite").parquet("data/bronze/order_items")

logger.info("Bronze layer completed successfully")
spark.stop()