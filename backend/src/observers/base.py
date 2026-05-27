from abc import ABC, abstractmethod


class PipelineObserver(ABC):

    @abstractmethod
    def on_node_enter(
        self,
        run_id: str,
        node: str
    ) -> None:
        pass

    @abstractmethod
    def on_node_exit(
        self,
        run_id: str,
        node: str,
        status: str,
        meta: dict = None
    ) -> None:
        pass