# policies/decision.py
from dataclasses import dataclass
from typing import Optional


@dataclass
class Decision:
    consumer_id: int
    prefetch: int
    action: Optional[str] = None