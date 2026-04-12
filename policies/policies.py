from policies.base_policy import BaseDecisionPolicy
from typing import Any, Dict, List
from execution.clients.consumer import Consumer
from DTOs.decision import Decision
from DTOs.metrics_dtos import AllHistory


class FirstPolicy(BaseDecisionPolicy):
    @classmethod
    def decide(self, allHistory:AllHistory ) -> List[Decision]:
        decisions = []
       
        new_prefetch = allHistory.raw_history[0][len(allHistory.raw_history[0]) - 1].prefetch_count + 1
        decisions.append(Decision(consumer_id=0, prefetch=new_prefetch))
        
        return decisions
    
    
class SecondPolicy(BaseDecisionPolicy):
    def decide(self, allHistory:AllHistory) -> Any:
        decisions = []
        for _id, metrics in allHistory.items():
            decisions.append(Decision(consumer_id=_id, prefetch=10))
        return decisions