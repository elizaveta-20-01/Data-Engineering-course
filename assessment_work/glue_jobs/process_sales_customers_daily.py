import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Ініціалізація Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

BUCKET = "s3://elizaveta-data-data-lake-224193575158"

# --- ПАЙПЛАЙН 1: process_sales (Daily) ---
sales = spark.read.option("header", "true").csv(f"{BUCKET}/raw/sales/*/*")
silver_sales = sales.select(
    col("CustomerId").cast("int").alias("client_id"),
    col("PurchaseDate").cast("date").alias("purchase_date"),
    col("Product").cast("string").alias("product_name"),
    col("Price").cast("double").alias("price")
)
silver_sales.write.mode("overwrite").partitionBy("purchase_date").parquet(f"{BUCKET}/silver/sales/")

# --- ПАЙПЛАЙН 2: process_customers (Daily) ---
customers = spark.read.option("header", "true").csv(f"{BUCKET}/raw/customers/*/*")
silver_customers = customers.select(
    col("Id").cast("int").alias("client_id"),
    col("FirstName").alias("first_name"),
    col("LastName").alias("last_name"),
    col("Email").alias("email"),
    col("RegistrationDate").cast("date").alias("registration_date"),
    col("State").alias("state")
)
silver_customers.write.mode("overwrite").parquet(f"{BUCKET}/silver/customers/")

job.commit()