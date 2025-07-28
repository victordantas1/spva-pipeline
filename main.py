from pyspark.sql.functions import from_json, col, get_json_object, concat_ws

from preprocessors import PreprocessorText
from schemas import debezium_payload_schema

from schemas import source_schema
from utils import Utils
from config import config
from loguru import logger

def main():
    spark = Utils.get_spark_session(config=config)

    logger.info('Starting Kafka Stream')
    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f'{config["kafka_host"]}:{config["kafka_port"]}') \
        .option("subscribe", config['kafka_topics']) \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info('Casting value field')
    df_value_string = df_kafka.selectExpr("CAST(value AS STRING)")

    logger.info('Extracting Payload')
    df_debezium_payload = df_value_string.select(
        get_json_object(col("value"), "$.payload").alias("payload"),
        get_json_object(col("value"), "$.source").alias("source")
    )

    logger.info('Transform payload with schema')
    df_parsed = df_debezium_payload.withColumn(
                                "parsed_payload", from_json(col("payload"), debezium_payload_schema),
                                    ) \
                                    .withColumn(
                                "parsed_source", from_json(col("source"), source_schema)
                                    )


    logger.info('Select after field')
    df = df_parsed.select(
        col("parsed_payload.op").alias("operacao"),
        col("parsed_payload.after.*"),
        col("parsed_payload.source.*")
    )

    logger.info('Preprocessing data')

    df = df.withColumn(config['text_column'], concat_ws(' ', col('title'), col('description')))

    preprocessor = PreprocessorText(config)
    preprocessor.load_pipeline(spark)

    df_preprocessed = preprocessor.process_df(df)

    logger.info('Writing data')

    """
    query = df_preprocessed.writeStream \
        .format("mongodb") \
        .option("spark.mongodb.database", "spva") \
        .option("spark.mongodb.collection", "jobs") \
        .outputMode("append") \
        .option("checkpointLocation", "C:/projects/spva-pipeline/spark_checkpoints") \
        .option("truncate", "false") \
        .start()
    """

    query = df_preprocessed.writeStream \
        .format("console") \
        .option("spark.mongodb.database", "spva") \
        .option("spark.mongodb.collection", "jobs") \
        .outputMode("append") \
        .option("checkpointLocation", "C:/projects/spva-pipeline/spark_checkpoints") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()