from config import create_spark_session
from logger_setup import get_logger
from pyspark.sql.functions import col

logger = get_logger(__name__)
logger.info("Silver layer started")

spark = create_spark_session("SilverLayer")

customers_df = spark.read.parquet("data/bronze/customers")
products_df = spark.read.parquet("data/bronze/products")
orders_df = spark.read.parquet("data/bronze/orders")
order_items_df = spark.read.parquet("data/bronze/order_items")

completed_orders_df = orders_df.filter(col("status") == "COMPLETED")

sales_df = completed_orders_df \
    .join(customers_df, on="customer_id", how="inner") \
    .join(order_items_df, on="order_id", how="inner") \
    .join(products_df, on="product_id", how="inner") \
    .withColumn("revenue", col("quantity") * col("price"))

sales_df.write.mode("overwrite").parquet("data/silver/sales")

logger.info("Silver layer completed successfully")
spark.stop()