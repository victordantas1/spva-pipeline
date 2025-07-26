from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from resumes_pipeline.model import JobDAO


class JobRepository:
    def __init__(self, session: Session):
        self.session = session
        
    def get_jobs(self) -> List[JobDAO]:
        stmt = select(JobDAO)
        result = self.session.execute(stmt)
        jobs = result.scalars().all()
        return list(jobs)
    
    def get_job_by_id(self, job_id: int) -> Optional[JobDAO]:
        return self.session.get(JobDAO, job_id)

    def save_job(self, job: JobDAO) -> JobDAO:
        try:
            self.session.add(job)
            self.session.commit()
            return job
        except:
            self.session.rollback()
            raise

    def delete_job_by_id(self, job_id: int) -> Optional[JobDAO]:
        try:
            job = self.get_job_by_id(job_id)
            self.session.delete(job)
            self.session.commit()
            return job
        except:
            self.session.rollback()
            raise

    def update_job_by_id(self, job_id: int, job: JobDAO) -> JobDAO:
        try:
            job_to_update = self.get_job_by_id(job_id)
            job_to_update.update_attributes(job)
            self.session.commit()
        except:
            self.session.rollback()
            raise
        return job_to_update