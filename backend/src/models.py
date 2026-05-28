from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, DateTime,
    Boolean, JSON, UniqueConstraint
)
from src.database import Base


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class BlogRun(Base):
    __tablename__ = 'blog_runs'

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String, unique=True, index=True, nullable=False)
    topic = Column(String, nullable=False)
    mode = Column(String, nullable=True)
    status = Column(String, default='PENDING')
    blog_title = Column(String, nullable=True)
    markdown_content = Column(Text, nullable=True)
    md_file_path = Column(String, nullable=True)
    workspace_dir = Column(String, nullable=True)
    word_count = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)


class ScheduledEmail(Base):
    __tablename__ = 'scheduled_emails'

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String, nullable=False, index=True)
    to_email = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    body = Column(Text, nullable=False)
    scheduled_at = Column(DateTime, nullable=False)
    status = Column(String, default='PENDING')
    sent_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class PipelineEvent(Base):
    __tablename__ = 'pipeline_events'

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String, nullable=False, index=True)
    node_name = Column(String, nullable=False)
    status = Column(String, nullable=False)
    meta = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class FailedJob(Base):
    __tablename__ = 'failed_jobs'

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String, nullable=False)
    task_name = Column(String, nullable=False)
    entity_id = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    attempts = Column(Integer, default=1)
    payload = Column(JSON, nullable=True)
    failed_at = Column(DateTime, default=datetime.utcnow)


class SectionAttempt(Base):
    __tablename__ = 'section_attempts'

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String, nullable=False, index=True)
    task_id = Column(String, nullable=False)
    status = Column(String, default='PENDING')
    attempts = Column(Integer, default=1)
    last_attempt_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('run_id', 'task_id', name='uq_run_task'),
    )