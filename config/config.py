import os

from dotenv import load_dotenv

load_dotenv()

config = {
    'db_url': os.environ.get('DATABASE_URL'),
    'db_username': os.environ.get('DATABASE_USER'),
    'db_password': os.environ.get('DATABASE_USER_PASSWORD'),
    'minio_endpoint': os.environ.get('MINIO_ENDPOINT'),
    'minio_access_key': os.environ.get('MINIO_ACCESS_KEY'),
    'minio_secret_key': os.environ.get('MINIO_SECRET_KEY'),
    'bucket_resumes': os.environ.get('BUCKET_RESUMES'),
    'text_column': os.environ.get('TEXT_COLUMN'),
    'pipeline_model_path': os.environ.get('PIPELINE_MODEL_PATH'),
}

auth_config = {
    'secret_key': os.environ.get('AUTH_SECRET_KEY'),
    'algorithm': os.environ.get('AUTH_ALGORITHM'),
    'access_token_expires_minutes': int(os.environ.get('AUTH_TOKEN_EXPIRATION_MINUTES'))
}