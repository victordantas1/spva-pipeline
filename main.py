from pyspark.sql.functions import from_json, col, get_json_object, concat_ws

from preprocessors import PreprocessorText
from service.resume_service import ResumeService

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
        .option("subscribe", config['kafka_topic']) \
        .option("startingOffsets", "earliest") \
        .load()

    logger.info('Casting value field')
    df_value_string = df_kafka.selectExpr("CAST(value AS STRING)")

    logger.info('Extracting Payload')
    df_debezium_payload = df_value_string.select(
            get_json_object(col("value"), "$.payload").alias("payload")
    )

    table = Utils.get_table(config['kafka_topic'])

    logger.info('Transform payload with schema')
    df_parsed = df_debezium_payload.withColumn(
    "parsed_payload", from_json(col("payload"), Utils.get_schema(table)),
    )

    logger.info('Select after field')
    df = df_parsed.select(
        col("parsed_payload.op").alias("operacao"),
        col("parsed_payload.after.*"),
        col("parsed_payload.source.*")
    )

    if table == 'user_app':
        resume_service = ResumeService(config)
        df = resume_service.get_resume_data_from_candidate(df)

    logger.info('Preprocessing data')

    df = df.withColumn(
        config['text_column'], concat_ws(' ', col('title'), col('description'))
    )

    preprocessor = PreprocessorText(config)
    preprocessor.load_pipeline(spark)

    df_preprocessed = preprocessor.process_df(df)

    logger.info('Writing data')

    query = df_preprocessed.writeStream \
        .format("mongodb") \
        .option("spark.mongodb.database", "spva") \
        .option("spark.mongodb.collection", "jobs") \
        .outputMode("append") \
        .option("checkpointLocation", "C:/projects/spva-pipeline/spark_checkpoints") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()