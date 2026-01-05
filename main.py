from execution.consumer_manager import ConsumerManager
from metrics.metrics_collector import Metrics
from control.brain import Brain
from control.decision_policy import AdaptivePolicy
def main():
    
    manager = ConsumerManager()
    metrics = Metrics(provider=manager)
    adaptive_policy = AdaptivePolicy()

    brain = Brain(consumer_manager=manager,metrics=metrics, adaptive_policy=adaptive_policy, interval=5.0)

    manager.start_consumers()
    metrics.start_collector()
    brain.start_control_loop()
