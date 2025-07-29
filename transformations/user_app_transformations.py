from config import config
from service.resume_service import ResumeService
from utils import Utils
from .commom_transformations import extract_payload
from .commom_transformations import transform as common_transform
from loguru import logger

def transform(batch_df, epoch_id):
    spark = Utils.get_spark_session(config)
    batch_df = extract_payload(batch_df, spark)
    service = ResumeService(config)

    print(f"--- Processing Batch ID: {epoch_id} ---")

    paths_to_download = [row.resume_path for row in batch_df.select("resume_path", "user_id").distinct().collect()]

    if not paths_to_download:
        logger.warning(f"Don't have resumes in this batch: {epoch_id}")
        return

    logger.info(f"Found {len(paths_to_download)} resumes to download")

    users_resume_data_list = []
    for path in paths_to_download:
        try:
            data = service.get_resume_data_from_candidate(path)
            users_resume_data_list.append((path, data))
        except Exception as e:
            print(f"Error processing path '{path}': {e}")

    df = spark.createDataFrame(users_resume_data_list, schema=["resume_path", "document"])

    batch_df = batch_df.join(df, on="resume_path", how="left")

    batch_df = common_transform(batch_df, epoch_id)

    batch_df \
        .write \
        .format('mongodb') \
        .mode("append") \
        .option("database", config['db']) \
        .option("collection", config['collection']) \
        .save()


