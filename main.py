from pyspark.sql.functions import from_json, col, get_json_object, concat_ws

from preprocessors import PreprocessorText
from service.resume_service import ResumeService

from utils import Utils
from config import config
from loguru import logger

from transformations.transformer import transform


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

    logger.info('Writing data')

    query = df_kafka.writeStream \
        .foreachBatch(transform) \
        .outputMode("append") \
        .option("checkpointLocation", "C:/projects/spva-pipeline/spark_checkpoints") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()