import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum as _sum, count as _count

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER = "s3://silver-512888886024-ap-southeast-2-an/"
GOLD = "s3://gold-512888886024-ap-southeast-2-an/"

# 1. Silver nunchi chaduvu ma - Clean files ma
orders_df = spark.read.parquet(SILVER + "orders/")
products_df = spark.read.parquet(SILVER + "products/")

print(f"Silver Orders: {orders_df.count()}, Products: {products_df.count()}")
orders_df.show(3)
products_df.show(3)

# 2. JOIN chey ma - product_id ni batti kalup ma
# O001 lo P013 undi - P013 ante enti? T-Shirt ah Laptop ah? Join cheste telustadi ma
joined_df = orders_df.join(products_df, on="product_id", how="left")
joined_df.show(5)

# 3. GOLD REPORT 1: City wise sales entha ma?
city_sales = joined_df.groupBy("city").agg(
    _sum("amount").alias("total_sales"),
    _count("order_id").alias("total_orders")
)
city_sales.show()
city_sales.write.mode("overwrite").parquet(GOLD + "city_sales/")

# 4. GOLD REPORT 2: Product wise entha mandi konnaru ma?
product_sales = joined_df.groupBy("product_name", "category").agg(
    _sum("amount").alias("total_sales"),
    _count("order_id").alias("times_sold")
)
product_sales.show()
product_sales.write.mode("overwrite").parquet(GOLD + "product_sales/")

print("GOLD DONE MA! Reports ready!")
job.commit()