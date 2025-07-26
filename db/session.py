from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from resumes_pipeline.config import config

def get_session():
    engine = create_engine(f"mysql+pymysql://{config['db_username']}:{config['db_password']}@{config['db_url']}")
    session = Session(bind=engine)
    """try:
        yield session
    finally:
        session.close()"""
    return session