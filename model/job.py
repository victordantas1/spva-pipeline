from sqlalchemy import ForeignKey, String, Text, Enum, Date
from sqlalchemy.orm import Mapped, mapped_column

from resumes_pipeline.model.base_model import Base
from .enums import CategoryEnum

from datetime import date


class JobDAO(Base):
    __tablename__ = 'job'
    job_id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey('user_app.user_id'), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    position: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[CategoryEnum] = mapped_column(Enum('remote', 'on-site', 'hybrid', name='category_enum'), nullable=False)
    create_date: Mapped[date] = mapped_column(Date, nullable=False)
    responsibilities: Mapped[str] = mapped_column(Text, nullable=True)
    requirements: Mapped[str] = mapped_column(Text, nullable=True)
    level: Mapped[str] = mapped_column(String(50), nullable=True)
    contract_type: Mapped[str] = mapped_column(String(50), nullable=True)
    schedule: Mapped[str] = mapped_column(String(50), nullable=True)
    salary_range: Mapped[str] = mapped_column(String(50), nullable=True)
    company: Mapped[str] = mapped_column(String(255), nullable=True)