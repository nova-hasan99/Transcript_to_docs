from sqlalchemy import create_engine, Column, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()

class FormatJob(Base):
    __tablename__ = "format_jobs"
    job_id = Column(String, primary_key=True)
    status = Column(String, default="queued")
    created_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)

# ✅ SQLite ডাটাবেজ কানেকশন
DB_PATH = os.path.join(os.path.dirname(__file__), "format_jobs.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


def cleanup_old_jobs(keep_last_n=10):
    """শেষ 10টা job রাখবে, পুরনো গুলো ডিলিট করবে"""
    db = SessionLocal()
    total_jobs = db.query(FormatJob).count()
    if total_jobs > keep_last_n:
        # পুরনো job গুলোর created_at অনুযায়ী sort করে delete
        subquery = (
            db.query(FormatJob.job_id)
            .order_by(FormatJob.created_at.desc())
            .offset(keep_last_n)
            .all()
        )
        old_ids = [jid[0] for jid in subquery]
        if old_ids:
            db.query(FormatJob).filter(FormatJob.job_id.in_(old_ids)).delete(synchronize_session=False)
            db.commit()
    db.close()
