import logging
from src.observers.base import PipelineObserver
from src.cache import set_pipeline_status, append_completed_node

logger = logging.getLogger(__name__)


class RedisStatusObserver(PipelineObserver):
    """
    Writes the current pipeline node to Redis.
    This enables the /status endpoint to return live progress
    without hitting PostgreSQL on every poll request.
    Redis reads take <1ms — perfect for polling every 2 seconds.
    """

    def on_node_enter(self, run_id: str, node: str) -> None:
        try:

            set_pipeline_status(run_id, node)
        except Exception as e:
            logger.error(f'RedisStatusObserver.on_node_enter failed: {e}')

    def on_node_exit(
        self,
        run_id: str,
        node: str,
        status: str,
        meta: dict
    ) -> None:
        try:
            if status == 'SUCCESS':
                
                append_completed_node(run_id, node)
        except Exception as e:
            logger.error(f'RedisStatusObserver.on_node_exit failed: {e}')