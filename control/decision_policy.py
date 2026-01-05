class AdaptivePolicy():
    def __init__(self, policy:str="default"):
        self.policy=policy
        #criar uma interface com os metodos de decisão
        #criar classes que implementem esses metodos de maneiras diferentes