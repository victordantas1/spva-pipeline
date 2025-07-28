from pyspark.sql import SparkSession, DataFrame
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