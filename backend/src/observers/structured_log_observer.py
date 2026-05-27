import json, logging
from datetime import datetime
from src.observers.base import PipelineObserver

logger = logging.getLogger(__name__)


class StructuredLogObserver(PipelineObserver):
    """
    Writes every event as a JSON log line.
    Unlike plain text logs, JSON logs can be:
    - Searched: grep '"node": "router_node"' logs.txt
    - Filtered by run_id: grep '"run_id": "abc-123"' logs.txt
    - Ingested by log management tools (Grafana Loki, CloudWatch Logs Insights)
    """

    def on_node_enter(self, run_id: str, node: str) -> None:
        # json.dumps converts the dict to a JSON string on one line
        logger.info(json.dumps({
            'event': 'node_started',
            'run_id': run_id,
            'node': node,
            'timestamp': datetime.utcnow().isoformat()
        }))

    def on_node_exit(
        self,
        run_id: str,
        node: str,
        status: str,
        meta: dict
    ) -> None:
        log_data = {
            'event': 'node_finished',
            'run_id': run_id,
            'node': node,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
        }

        
        log_data.update(meta)

        
        if status == 'FAILED':
            logger.error(json.dumps(log_data))
        else:
            logger.info(json.dumps(log_data))