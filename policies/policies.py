from policies.base_policy import BaseDecisionPolicy
from typing import Any, Dict, List
from execution.clients.consumer import Consumer
from DTOs.decision import Decision
from DTOs.metrics_dtos import AllHistory


class FirstPolicy(BaseDecisionPolicy):
    def decide(self, all_history: AllHistory) -> List[Decision]:
        decisions = []
       
        new_prefetch = all_history.raw_history[0][len(all_history.raw_history[0]) - 1].prefetch_count + 1
        decisions.append(Decision(consumer_id=0, prefetch=new_prefetch))
        
        return decisions
    
    
class SecondPolicy(BaseDecisionPolicy):
    def decide(self, all_history: AllHistory) -> Any:
        decisions = []
        for _id, metrics in all_history.items():
            decisions.append(Decision(consumer_id=_id, prefetch=10))
        return decisions
    
class BaseMessageQuantityPolicy():
    def __init__(self, target_quantity_message:int = 400):
        self.target_quantity_message = target_quantity_message

    def set_target_quantity_message(self, target_quantity_message: int) -> None:
        self.target_quantity_message = target_quantity_message
        print(f"[Policy] Novo target de mensagens definido: {self.target_quantity_message}")

    def decide(self, data) -> Any:
        quantity_message = len(data)
        print(f"Quantity of messages: {quantity_message} | target: {self.target_quantity_message}")
        if quantity_message > self.target_quantity_message * 1.1:
            return 0
        elif quantity_message < self.target_quantity_message * 0.9:
            return 0
        else:   
            return 0