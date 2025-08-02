from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType

transaction_schema = StructType([
    StructField("id", StringType(), True),
    StructField("total_order", LongType(), True),
    StructField("data_collection_order", LongType(), True)
])

job_schema = StructType([
    StructField("job_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("position", StringType(), True),
    StructField("category", StringType(), True),
    StructField("create_date", IntegerType(), True),
    StructField("responsibilities", StringType(), True),
    StructField("requirements", StringType(), True),
    StructField("level", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("schedule", StringType(), True),
    StructField("salary_range", StringType(), True),
    StructField("company", StringType(), True)
])

user_app_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("resume_path", StringType(), True),
])

