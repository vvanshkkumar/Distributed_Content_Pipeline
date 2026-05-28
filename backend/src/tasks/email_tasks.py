import json, logging
from datetime import datetime, timezone
from src.celery_app import celery_app
from src.database import SessionLocal
from src.models import ScheduledEmail, FailedJob
from src.email.strategy import email_strategy_factory

logger = logging.getLogger(__name__)

@celery_app.task(name='src.tasks.email_tasks.dispatch_due_emails')
def dispatch_due_emails():
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        
        due_emails = db.query(ScheduledEmail).filter(
            ScheduledEmail.scheduled_at <= now,
            ScheduledEmail.status == 'PENDING'
        ).all()
        
        dispatched = 0
        for email in due_emails:
            email.status = 'QUEUED'
            db.commit()
            
            send_scheduled_email.delay(email.id)
            dispatched += 1
            
        if dispatched > 0:
            logger.info(json.dumps({
                'event': 'emails_dispatched',
                'count': dispatched,
                'timestamp': now.isoformat()
            }))
            
    except Exception as e:
        logger.error(f'dispatch_due_emails error: {e}')
    finally:
        db.close()

@celery_app.task(
    name='src.tasks.email_tasks.send_scheduled_email',
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True
)
def send_scheduled_email(self, email_id: int):
    db = SessionLocal()
    try:
        record = db.query(ScheduledEmail).filter(
            ScheduledEmail.id == email_id
        ).first()
        
        if not record:
            logger.error(f'ScheduledEmail {email_id} not found skipping')
            return
            
        if record.status == 'SENT':
            logger.info(f'Email {email_id} already sent skipping duplicate')
            return
            
        strategy = email_strategy_factory()
        
        strategy.send(
            to=record.to_email,
            subject=record.subject,
            body=record.body
        )
        
        record.status = 'SENT'
        record.sent_at = datetime.now(timezone.utc)
        db.commit()
        
        logger.info(json.dumps({
            'event': 'email_sent',
            'email_id': email_id,
            'to': record.to_email
        }))
        
    except Exception as exc:
        attempt = self.request.retries + 1
        logger.warning(json.dumps({
            'event': 'email_send_failed',
            'email_id': email_id,
            'attempt': attempt,
            'error': str(exc)
        }))
        
        if self.request.retries > self.max_retries:
            try:
                db.add(FailedJob(
                    task_id=self.request.id,
                    task_name='send_scheduled_email',
                    entity_id=email_id,
                    error_message=str(exc),
                    attempts=attempt,
                    payload={'email_id': email_id}
                ))
                
                record = db.query(ScheduledEmail).get(email_id)
                if record:
                    record.status = 'FAILED'
                    record.error_message = str(exc)
                    db.commit()
                    
                logger.error(json.dumps({
                    'event': 'email_moved_to_dlq',
                    'email_id': email_id,
                    'total_attempts': attempt
                }))
            except Exception as db_err:
                logger.error(f'Failed to write to DLQ: {db_err}')
        else:
            countdown = 60 * (2 ** self.request.retries)
            raise self.retry(exc=exc, countdown=countdown)
    finally:
        db.close()