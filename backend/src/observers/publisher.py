import logging
from typing import List
from src.observers.base import PipelineObserver

logger = logging.getLogger(__name__)


class PipelineEventPublisher:
    """
    Holds a list of observers and notifies all of them on each event.

    Key design choice: exceptions in one observer are caught here.
    This means one failing observer (e.g. Redis is down) never prevents
    the others (audit log, structured log) from running.
    """

    def __init__(self):
       
        self._observers: List[PipelineObserver] = []

    def attach(self, observer: PipelineObserver) -> None:
        """Add an observer to the notification list."""
        self._observers.append(observer)
        logger.info(f'Observer attached: {type(observer).__name__}')

    def on_node_enter(self, run_id: str, node: str) -> None:
        """Notify all observers that a node has started."""
        for observer in self._observers:
            try:
                observer.on_node_enter(run_id, node)
            except Exception as e:
           
                logger.error(
                    f'{type(observer).__name__}.on_node_enter failed: {e}'
                )

    def on_node_exit(
        self,
        run_id: str,
        node: str,
        status: str,
        meta: dict = None
    ) -> None:
        """Notify all observers that a node has finished."""
        for observer in self._observers:
            try:
                observer.on_node_exit(
                    run_id,
                    node,
                    status,
                    meta
                )
            except Exception as e:
                logger.error(
                    f'{type(observer).__name__}.on_node_exit failed: {e}'
                )


publisher = PipelineEventPublisher()