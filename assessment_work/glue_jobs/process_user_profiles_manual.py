import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Ініціалізація Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

BUCKET = "s3://elizaveta-data-data-lake-224193575158"

# --- ПАЙПЛАЙН 3: process_user_profiles (Manual) ---
profiles = spark.read.json(f"{BUCKET}/raw/user_profiles/*")
profiles.write.mode("overwrite").parquet(f"{BUCKET}/silver/user_profiles/")

job.commit()