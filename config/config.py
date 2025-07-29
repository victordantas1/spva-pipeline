import os

from dotenv import load_dotenv

load_dotenv()

config = {
    # MinIO
    'minio_endpoint': os.environ.get('MINIO_ENDPOINT'),
    'minio_access_key': os.environ.get('MINIO_ACCESS_KEY'),
    'minio_secret_key': os.environ.get('MINIO_SECRET_KEY'),
    'bucket_resumes': os.environ.get('BUCKET_RESUMES'),

    # Preprocessing
    'text_column': os.environ.get('TEXT_COLUMN'),
    'pipeline_model_path': os.environ.get('PIPELINE_MODEL_PATH'),

    # Mongo DB
    'mongo_uri': os.environ.get('MONGO_URI'),
    'db': os.environ.get('MONGO_DATABASE'),
    'collection': os.environ.get('COLLECTION'),

    # Kafka
    'kafka_host': os.environ.get('KAFKA_HOST'),
    'kafka_port': os.environ.get('KAFKA_PORT'),
    'kafka_topic': os.environ.get('KAFKA_TOPIC'),
}