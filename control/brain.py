#criar interfaces e tipar as injeções

class Brain():
    def __init__(self, consumer_manager, metrics, adaptive_policy, interval:int=5.0):
        self.manager = consumer_manager
        self.metrics = metrics
        self.adaptive_policy = adaptive_policy
        self.interval = interval
        
        
    def start(self):
        pass
        #cria uma thread que chama o brain loop e da start nela
        
        
    def bain_loop(self):
        pass
        #while True
        #pega as metricas
        #Envia as metricas para adptive_policy
        #Envia a resposta para consumer manager