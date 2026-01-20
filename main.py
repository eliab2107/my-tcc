from control import brain
from execution.consumer_manager import ConsumerManager
from metrics.metrics_collector import Metrics
from control.brain import Brain
from policies.decide_policy import DecidePolicy
import argparse
from time import sleep
def main():
    parser = argparse.ArgumentParser(description="Adaptive Prefetch Controller")
    parser.add_argument(
        "--policy_name",
        type=str,
        default="default",
        help="Policy de decisão (first | second | rl)"
    )
    parser.add_argument(
        "--num_consumers",
        type=int,
        default=1,
        help="Número de consumers"
    )
    args = parser.parse_args()

    manager = ConsumerManager(num_consumers=args.num_consumers)
    metrics = Metrics(provider=manager)
    policy  = DecidePolicy.decide_police(args.policy_name)
    

    brain = Brain(consumer_manager=manager,metrics=metrics, adaptive_policy=policy, interval=5.0)

    manager.start_consumers()
    metrics.start_collector()
    brain.start()
    return brain


if __name__ == "__main__":
    brain = main()
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print("Encerrando...")
        brain.stop()