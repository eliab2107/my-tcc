from abc import ABC, abstractmethod

class ConsumerMetricsProvider(ABC):
    @abstractmethod
    def snapshot_metrics(self) -> dict:  
        ...