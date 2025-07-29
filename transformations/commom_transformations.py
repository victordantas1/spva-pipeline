from pyspark.sql.functions import get_json_object, col, from_json, concat_ws

from config import config
from preprocessors import PreprocessorText
from utils import Utils
from loguru import logger

def extract_payload(batch_df, spark):

    logger.info('Casting value field')
    df_value_string = batch_df.selectExpr("CAST(value AS STRING)")

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

    return df

def transform(batch_df, epoch_id):
    spark = Utils.get_spark_session(config)
    logger.info('Preprocessing data')

    table = Utils.get_table(config['kafka_topic'])

    if table == 'job':
        batch_df = extract_payload(batch_df, spark)
        batch_df = batch_df.withColumn(
            config['text_column'], concat_ws(' ', col('title'), col('description'))
        )

    preprocessor = PreprocessorText(config)
    preprocessor.load_pipeline(spark)

    df_preprocessed = preprocessor.process_df(batch_df)
    return df_preprocessed