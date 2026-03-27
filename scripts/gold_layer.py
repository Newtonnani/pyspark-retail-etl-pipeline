from config import create_spark_session
from logger_setup import get_logger
from pyspark.sql.functions import sum, count

logger = get_logger(__name__)
logger.info("Gold layer started")

spark = create_spark_session("GoldLayer")

sales_df = spark.read.parquet("data/silver/sales")

product_sales_df = sales_df.groupBy("product_name").agg(
    sum("revenue").alias("total_sales"),
    sum("quantity").alias("total_quantity")
)

customer_sales_df = sales_df.groupBy("customer_name").agg(
    sum("revenue").alias("customer_lifetime_value"),
    count("order_id").alias("total_orders")
)

state_sales_df = sales_df.groupBy("state").agg(
    sum("revenue").alias("state_revenue")
)

product_sales_df.write.mode("overwrite").parquet("data/gold/product_sales")
customer_sales_df.write.mode("overwrite").parquet("data/gold/customer_sales")
state_sales_df.write.mode("overwrite").parquet("data/gold/state_sales")

logger.info("Gold layer completed successfully")
spark.stop()