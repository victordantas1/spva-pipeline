from pyspark.sql import SparkSession

class Utils:
    @staticmethod
    def get_spark_session(config: dict) -> SparkSession:
        spark = SparkSession.builder\
            .appName("EDA Resumes") \
            .master("local[*]")\
            .config("spark.driver.memory", "6g") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()
        return spark