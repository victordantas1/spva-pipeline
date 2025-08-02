from src.config import config
from .job_transformations import transform as job_transform
from .user_app_transformations import transform as user_app_transform
from loguru import logger

def transform(batch_df, epoch_id):
    collection = config['collection']

    logger.info(f"---- Processing batch {epoch_id} ----")

    if collection == 'job':
        job_transform(batch_df, epoch_id)
    else:
        user_app_transform(batch_df, epoch_id)