import fitz
from minio import S3Error

from repository.resume_repository import ResumeRepository
from utils import Utils


class ResumeService:
    def __init__(self, config: dict) -> None:
        self.repository = ResumeRepository(Utils.get_minio_client(config))
        self.bucket_name = config["bucket_resumes"]

    def get_resume_data_from_candidate(self, path) -> str:
        full_text = ""
        try:
            resume_data = self.repository.get_resume(bucket_name=self.bucket_name, object_name=path)

            with fitz.open(stream=resume_data.read(), filetype="pdf") as doc:
                print(f"O documento tem {len(doc)} páginas.")

                for page_num, page in enumerate(doc, start=1):
                    text = page.get_text()
                    full_text += text
        except S3Error as e:
            print(e)

        return full_text