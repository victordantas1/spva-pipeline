from config import config
from utils import Utils
from .job_transformations import transform as job_transform
from .user_app_transformations import transform as user_app_transform

def transform(batch_df, epoch_id):
    collection = config['collection']

    if collection == 'job':
        job_transform(batch_df, epoch_id)
    else:
        user_app_transform(batch_df, epoch_id)