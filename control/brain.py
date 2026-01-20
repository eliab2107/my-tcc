import time
from threading import Thread
from typing import Dict, Any
from execution.consumer_manager import ConsumerManager
from metrics.metrics_collector import Metrics
from policies.base_policy import BaseDecisionPolicy
from DTOs.metrics_dtos import AllHistory

class Brain:
    """
    Orquestrador do loop de controle adaptativo.
    """

    def __init__( self, consumer_manager:ConsumerManager, metrics:Metrics, adaptive_policy:BaseDecisionPolicy, interval: float = 5.0):
        self.consumer_manager = consumer_manager
        self.metrics = metrics
        self.policy = adaptive_policy
        self.interval = interval

        self._running = False
        self._thread: Thread | None = None

    def start(self):
        """
        Inicia o loop adaptativo em uma thread separada.
        """
        if self._running:
            return

        self._running = True
        self._thread = Thread(
            target=self.control_loop,
            daemon=True,
            name="brain-loop"
        )
        self._thread.start()

        print("[BRAIN] Loop de controle adaptativo iniciado")

    def stop(self):
        """
        Encerra o loop de controle.
        """
        self._running = False
        print("[BRAIN] Loop de controle adaptativo finalizado")

    def control_loop(self):
        """
        Loop principal:
        - coleta métricas
        - consulta policy
        - aplica decisões
        """
        while self._running:
            time.sleep(self.interval)

            metrics_snapshot:AllHistory = self.metrics.get_all_from_system()
    
            if not metrics_snapshot:
                continue
           
            try:
                decisions = self.policy.decide(metrics_snapshot)
            except Exception as e:
                print(f"[BRAIN] Erro na policy: {e}")
                continue

            if not decisions:
                continue
        
            try:
                self.consumer_manager.update_prefetchs(decisions)
                
            except Exception as e:
                print(
                    f"[BRAIN] Erro ao aplicar decisão no consumer "
                    f"{e}"
                )
