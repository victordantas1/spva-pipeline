from pyspark.sql.functions import col, length

from src.config import config
from src.service.resume_service import ResumeService
from src.utils import Utils
from .commom_transformations import extract_payload
from .commom_transformations import transform as common_transform
from loguru import logger

def filter_by_resume_path(batch_df):
    batch_df = batch_df.filter(col('resume_path').isNotNull())
    return batch_df

def get_resume_df(batch_df, epoch_id):
    spark = Utils.get_spark_session(config)
    service = ResumeService(config)

    paths_to_download = [row.resume_path for row in batch_df.select("resume_path", "user_id").distinct().collect()]

    if not paths_to_download:
        logger.warning(f"Don't have resumes in this batch: {epoch_id}")
        return

    logger.info(f"Found {len(paths_to_download)} resumes to download")

    users_resume_data_list = []
    for path in paths_to_download:
        try:
            logger.info(f"Downloading {path}")
            data = service.get_resume_data_from_candidate(path)
            users_resume_data_list.append((path, data))
        except Exception as e:
            print(f"Error processing path '{path}': {e}")

    df = spark.createDataFrame(users_resume_data_list, schema=["resume_path", "document"])

    df = df.filter((col('resume_path').isNotNull()) & (length(col('document')) > 0))

    return df

def transform(batch_df, epoch_id):
    batch_df = extract_payload(batch_df)
    batch_df = filter_by_resume_path(batch_df)

    batch_df.cache()

    if batch_df.isEmpty():
        logger.warning(f"Batch {epoch_id}: No one resume found. Dropping Batch.")
        batch_df.unpersist()
        return

    resumes_df = get_resume_df(batch_df, epoch_id)

    resumes_df.cache()
    if resumes_df.isEmpty():
        logger.warning(f"Batch {epoch_id}: No one resume found. Dropping Batch.")
        batch_df.unpersist()
        return
    resumes_df.unpersist()

    batch_df = batch_df.join(resumes_df, on="resume_path", how="left")
    batch_df = batch_df.filter((col('resume_path').isNotNull()) & (col('document').isNotNull()))

    common_transform(batch_df, epoch_id)




