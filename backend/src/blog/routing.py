import os
import json
import logging
from typing import List, Optional
from datetime import datetime
import markdown

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Request
from sqlalchemy.orm import Session
from sqlalchemy import text, desc
from pydantic import BaseModel

from src.database import get_db
from src.models import User, BlogRun, PipelineEvent, SectionAttempt, ScheduledEmail, FailedJob
from src.auth.jwt_handler import get_current_user
from src.blog.service import run_blog_generation
from src.tasks.email_tasks import send_scheduled_email
from src.cache import (
    check_rate_limit, 
    get_cached_recents, 
    set_cached_recents,
    get_cached_preview,
    set_cached_preview,
    get_pipeline_status,
    get_completed_nodes
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=['Blog and System'])

# ==========================================
# Pydantic Schemas
# ==========================================
class GenerateRequest(BaseModel):
    topic: str
    to_email: Optional[str] = None
    schedule_at: Optional[str] = None

class SendExistingRequest(BaseModel):
    run_id: str
    to_email: str

class ScheduleExistingRequest(BaseModel):
    run_id: str
    to_email: str
    scheduled_at: str

# ==========================================
# Public Endpoints (No JWT Required)
# ==========================================

@router.get('/api/health/')
def health_check(db: Session = Depends(get_db)):
    """Used by AWS ALB to verify instance health."""
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "error", "database": "unreachable"}

@router.get('/api/recents/')
def get_recents(db: Session = Depends(get_db)):
    """Returns the last 10 blog runs. Checks Redis cache first."""
    cached = get_cached_recents()
    if cached:
        return json.loads(cached)

    runs = db.query(BlogRun).order_by(desc(BlogRun.created_at)).limit(10).all()
    
    results = []
    for r in runs:
        results.append({
            "run_id": r.run_id,
            "topic": r.topic,
            "blog_title": r.blog_title,
            "status": r.status,
            "created_at": r.created_at.isoformat() if r.created_at else None
        })
    
    # Store in Redis for 5 minutes
    set_cached_recents(json.dumps(results))
    return results

@router.get('/api/blog/runs/{run_id}/status')
def get_run_status(run_id: str, db: Session = Depends(get_db)):
    """Live progress endpoint. Reads current node from Redis, history from PostgreSQL."""
    # 1. Fast read from Redis for the current executing node
    live_status = get_pipeline_status(run_id)
    current_node = live_status.get('current_node', 'starting')
    
    # 2. Get history from PostgreSQL Audit Log
    events = db.query(PipelineEvent).filter(PipelineEvent.run_id == run_id).order_by(PipelineEvent.created_at).all()
    history = [{
        "node": e.node_name, 
        "status": e.status, 
        "timestamp": e.created_at.isoformat(), 
        "meta": e.meta
    } for e in events]

    # Calculate progress (6 total nodes in LangGraph)
    completed_nodes = get_completed_nodes(run_id)
    progress_pct = min(int((len(completed_nodes) / 6) * 100), 100)

    # Override to 100% if the database says it's strictly SUCCESS or FAILED
    run_record = db.query(BlogRun).filter(BlogRun.run_id == run_id).first()
    if run_record and run_record.status in ["SUCCESS", "FAILED"]:
        progress_pct = 100

    return {
        "run_id": run_id,
        "progress_pct": progress_pct,
        "current_node": current_node,
        "completed_nodes": completed_nodes,
        "history": history
    }

@router.get('/api/blog/runs/{run_id}/sections')
def get_run_sections(run_id: str, db: Session = Depends(get_db)):
    """Kafka worker progress for the Streamlit UI."""
    sections = db.query(SectionAttempt).filter(SectionAttempt.run_id == run_id).all()
    
    sec_list = []
    completed = 0
    in_progress = 0
    
    for s in sections:
        sec_list.append({
            "task_id": s.task_id,
            "status": s.status,
            "attempts": s.attempts
        })
        if s.status == "DONE":
            completed += 1
        elif s.status == "PROCESSING":
            in_progress += 1

    return {
        "sections": sec_list,
        "total": len(sections),
        "completed": completed,
        "in_progress": in_progress
    }

@router.get('/api/blog/runs/{run_id}/preview')
def preview_blog(run_id: str, db: Session = Depends(get_db)):
    """Converts the finished markdown to HTML. Uses permanent Redis caching."""
    cached_html = get_cached_preview(run_id)
    if cached_html:
        return {"html": cached_html}

    run_record = db.query(BlogRun).filter(BlogRun.run_id == run_id).first()
    if not run_record or not run_record.md_file_path:
        raise HTTPException(status_code=404, detail="Blog not found or not finished.")

    # Read from S3 or Local Disk
    try:
        path = run_record.md_file_path
        if path.startswith("s3://"):
            import boto3
            s3 = boto3.client('s3', region_name=os.getenv("AWS_REGION", "us-east-1"))
            bucket = path.split("/")[2]
            key = "/".join(path.split("/")[3:])
            obj = s3.get_object(Bucket=bucket, Key=key)
            md_content = obj['Body'].read().decode('utf-8')
        else:
            with open(path, "r", encoding="utf-8") as f:
                md_content = f.read()

        html_content = markdown.markdown(md_content)
        set_cached_preview(run_id, html_content)
        return {"html": html_content}
    except Exception as e:
        logger.error(f"Failed to load preview: {e}")
        raise HTTPException(status_code=500, detail="Failed to load blog file.")

# ==========================================
# Protected Endpoints (JWT Required)
# ==========================================

@router.post('/api/blog/generate')
def generate_blog(
    request: GenerateRequest, 
    http_req: Request,
    bg_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Starts the LangGraph pipeline in a background task."""
    client_ip = http_req.client.host
    if not check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit: max 3 per minute")

    import uuid
    run_id = str(uuid.uuid4())

    # Create the BlogRun record so the Streamlit UI can immediately poll it
    new_run = BlogRun(run_id=run_id, topic=request.topic, status="PENDING")
    db.add(new_run)
    db.commit()

    # Pass LangGraph execution to a background task so HTTP returns instantly
    bg_tasks.add_task(run_blog_generation, request.topic, run_id)

    # If the user requested an email schedule at generation time
    if request.to_email:
        if request.schedule_at:
            dt = datetime.fromisoformat(request.schedule_at)
            email_task = ScheduledEmail(
                run_id=run_id,
                to_email=request.to_email,
                subject=f"Your Blog: {request.topic}",
                body=f"Your generated blog will be available at run ID: {run_id}",
                scheduled_at=dt,
                status="PENDING"
            )
            db.add(email_task)
            db.commit()

    return {"message": "Generation started", "run_id": run_id}

@router.post('/api/blog/send-existing')
def send_existing_blog(
    request: SendExistingRequest, 
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Sends a completed blog via email immediately."""
    run_record = db.query(BlogRun).filter(BlogRun.run_id == request.run_id).first()
    if not run_record:
        raise HTTPException(status_code=404, detail="Blog run not found.")

    email_task = ScheduledEmail(
        run_id=request.run_id,
        to_email=request.to_email,
        subject=f"Your Blog: {run_record.blog_title or run_record.topic}",
        body=f"Read your blog here: /api/blog/runs/{request.run_id}/preview",
        scheduled_at=datetime.utcnow(), 
        status="PENDING"
    )
    db.add(email_task)
    db.commit()

    # Queue in Celery immediately
    send_scheduled_email.delay(email_task.id)
    
    return {"message": "Email queued for delivery."}

@router.post('/api/blog/schedule-existing')
def schedule_existing_blog(
    request: ScheduleExistingRequest, 
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Schedules a completed blog for future email delivery."""
    try:
        dt = datetime.fromisoformat(request.scheduled_at)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid datetime format. Use ISO 8601.")

    email_task = ScheduledEmail(
        run_id=request.run_id,
        to_email=request.to_email,
        subject="Scheduled Blog Delivery",
        body=f"Your scheduled blog is ready: /api/blog/runs/{request.run_id}/preview",
        scheduled_at=dt,
        status="PENDING"
    )
    db.add(email_task)
    db.commit()
    
    return {"message": "Email scheduled successfully."}

@router.get('/api/blog/scheduled')
def get_scheduled_emails(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Returns all scheduled emails."""
    emails = db.query(ScheduledEmail).order_by(desc(ScheduledEmail.created_at)).limit(50).all()
    return [{"id": e.id, "to_email": e.to_email, "scheduled_at": e.scheduled_at.isoformat(), "status": e.status} for e in emails]

@router.get('/api/jobs/failed')
def get_failed_jobs(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Returns all jobs in the dead-letter queue."""
    jobs = db.query(FailedJob).order_by(desc(FailedJob.failed_at)).all()
    return [{
        "id": j.id, 
        "task_name": j.task_name, 
        "attempts": j.attempts, 
        "error": j.error_message, 
        "failed_at": j.failed_at.isoformat() if j.failed_at else None
    } for j in jobs]

@router.post('/api/jobs/failed/{job_id}/retry')
def retry_failed_job(job_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Re-queues a failed job and removes it from the DLQ."""
    job = db.query(FailedJob).filter(FailedJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Failed job not found.")

    if job.task_name == "send_scheduled_email":
        # Reset the status of the email so it can be retried
        email = db.query(ScheduledEmail).filter(ScheduledEmail.id == job.entity_id).first()
        if email:
            email.status = "QUEUED"
            db.commit()
            
        send_scheduled_email.delay(job.entity_id)
    
    # Remove from Dead Letter Queue
    db.delete(job)
    db.commit()

    return {"message": "Job re-queued successfully."}