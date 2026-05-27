import logging
from datetime import datetime
from src.observers.base import PipelineObserver
from src.database import SessionLocal
from src.models import PipelineEvent

logger = logging.getLogger(__name__)


class AuditLogObserver(PipelineObserver):
    """
    Creates a permanent, queryable record of every pipeline step.
    Powers the history array in GET /api/blog/runs/{run_id}/status.
    Records are NEVER updated — only inserted (append-only audit trail).
    """

    def on_node_enter(self, run_id: str, node: str) -> None:
    
        db = SessionLocal()
        try:
            db.add(PipelineEvent(
                run_id=run_id,
                node_name=node,
                status='RUNNING',
                created_at=datetime.utcnow()
            ))
            db.commit()
        except Exception as e:
            
            logger.error(f'AuditLogObserver.on_node_enter failed: {e}')
        finally:
            db.close()

    def on_node_exit(
        self,
        run_id: str,
        node: str,
        status: str,
        meta: dict = None
    ) -> None:
        db = SessionLocal()
        try:
            db.add(PipelineEvent(
                run_id=run_id,
                node_name=node,
                status=status,
                meta=meta or {},
                created_at=datetime.utcnow()
            ))
            db.commit()
        except Exception as e:
            logger.error(f'AuditLogObserver.on_node_exit failed: {e}')
        finally:
            db.close()