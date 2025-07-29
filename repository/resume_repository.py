from io import BytesIO

from minio import Minio
from urllib3 import BaseHTTPResponse


class ResumeRepository:

    def __init__(self, client: Minio):
        self.client = client

    def get_resume(self, bucket_name: str, object_name: str) -> BaseHTTPResponse:
        print(object_name)
        data = self.client.get_object(bucket_name, object_name=object_name)
        return data