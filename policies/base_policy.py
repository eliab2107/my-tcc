from abc import ABC, abstractmethod
from typing import Any, Dict, List
from execution.clients.consumer import Consumer
from DTOs.decision import Decision 
from DTOs.metrics_dtos import AllHistory
class BaseDecisionPolicy(ABC):
    """
    Interface base para políticas de decisão.
    """

    @abstractmethod
    def decide(self, metrics_snapshot: AllHistory) -> List[Decision]:
        """
        Recebe um snapshot de métricas e retorna uma decisão
        de controle (ex: ajuste de prefetch).
        A decisão NÃO deve ser aplicada aqui.
        """
        pass
