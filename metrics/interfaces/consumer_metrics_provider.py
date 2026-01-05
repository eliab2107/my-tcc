from typing import Protocol

class ConsumerMetricsProvider(Protocol):
    def snapshot_metrics(self) -> dict:  
        ...