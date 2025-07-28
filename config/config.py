import os

from dotenv import load_dotenv

load_dotenv()

config = {
    'minio_endpoint': os.environ.get('MINIO_ENDPOINT'),
    'minio_access_key': os.environ.get('MINIO_ACCESS_KEY'),
    'minio_secret_key': os.environ.get('MINIO_SECRET_KEY'),
    'bucket_resumes': os.environ.get('BUCKET_RESUMES'),
    'text_column': os.environ.get('TEXT_COLUMN'),
    'pipeline_model_path': os.environ.get('PIPELINE_MODEL_PATH'),
    'mongo_uri': os.environ.get('MONGO_URI'),
    'kafka_host': os.environ.get('KAFKA_HOST'),
    'kafka_port': os.environ.get('KAFKA_PORT'),
    'kafka_topics': os.environ.get('KAFKA_TOPICS'),
}