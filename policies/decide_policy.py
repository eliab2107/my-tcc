from policies.policies import FirstPolicy, SecondPolicy
from policies.base_policy import BaseDecisionPolicy


class DecidePolicy():
    def decide_police(policy_name:str="default") -> BaseDecisionPolicy:
        match policy_name:
            case "first":
                return FirstPolicy
            
            case "second":
                return SecondPolicy
            
            case _:
                return FirstPolicy