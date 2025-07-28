from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, LongType, StringType, StructType

from schemas import job_schema, user_app_schema, source_schema, transaction_schema
from .jobs_data import jobs_data

class Utils:
    @staticmethod
    def get_spark_session(config: dict) -> SparkSession:
        spark = SparkSession.builder\
            .appName("EDA Resumes") \
            .master("local[*]")\
            .config("spark.driver.memory", "6g") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
            .config("spark.mongo.write.connection.uri", config["mongo_uri"]) \
            .getOrCreate()
        return spark

    @staticmethod
    def get_train_df(spark: SparkSession) -> DataFrame:
        train_df = spark.createDataFrame(jobs_data)
        return train_df

    @staticmethod
    def get_schema(table_name: str) -> StructType:
        debezium_payload_schema = StructType([
            StructField("before", job_schema if table_name == 'job' else user_app_schema, True),
            StructField("after", job_schema if table_name == 'job' else user_app_schema, True),
            StructField("source", StructType([
                    StructField("table", StringType(), True)
                ]), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", transaction_schema, True)
        ])

        return debezium_payload_schema

    @staticmethod
    def get_table(topic: str) -> str:
        table_name = topic.split('.')[-1]
        return table_name