from dotenv import load_dotenv
import pika
import json
import time
import os
from threading import Lock

load_dotenv()


class Consumer:
    def __init__(self, id: int, prefetch_count: int = 1, task_level: int = 1):
        self.id = id
        self.task_level = task_level

        self.host = os.getenv("RABBITMQ_HOST")
        self.user = os.getenv("RABBITMQ_USER")
        self.password = os.getenv("RABBITMQ_PASS")

        credentials = pika.PlainCredentials(self.user, self.password)
        parametros = pika.ConnectionParameters(host=self.host, credentials=credentials)

        self.connection = pika.BlockingConnection(parametros)
        self.channel = self.connection.channel()
        self.queue = os.getenv("RABBITMQ_QUEUE", "fila_teste")
        self.channel.queue_declare(queue=self.queue)

        self._prefetch_count = prefetch_count
        self.channel.basic_qos(prefetch_count=self._prefetch_count)
        self._consumer_tag = self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.callback,
            auto_ack=False,
        )

        self._metrics: list = []
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Prefetch — propriedade thread-safe
    # ------------------------------------------------------------------
    @property
    def prefetch_count(self) -> int:
        with self._lock:
            return self._prefetch_count

    def set_new_prefetch_count(self, ajuste: int) -> None:
        """Chamado pelo Manager (thread externa). Agenda a mudança na thread da conexão."""
        with self._lock:
            novo = max(1, self._prefetch_count + ajuste)
        self.connection.add_callback_threadsafe(
            lambda: self._recreate_consumer(novo)
        )

    def _recreate_consumer(self, novo_valor: int) -> None:
        """
        Única forma confiável de mudar prefetch em runtime no RabbitMQ:
        cancela o consumer atual, aplica QoS e recria.
        Sempre executado na thread dona da conexão.
        """
        self.channel.basic_cancel(self._consumer_tag)
        self.channel.basic_qos(prefetch_count=novo_valor)
        self._consumer_tag = self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self.callback,
            auto_ack=False,
        )
        with self._lock:
            old = self._prefetch_count
            self._prefetch_count = novo_valor
        print(f"[Consumer {self.id}] prefetch_count: {old} → {novo_valor}")

    # ------------------------------------------------------------------
    # Consumo
    # ------------------------------------------------------------------
    def callback(self, ch, method, properties, body):
        start = time.monotonic()
        message = json.loads(body)
        self.process_message(message)
        elapsed = time.monotonic() - start

        # Latência end-to-end: tempo desde a criação da mensagem até agora.
        # Requer clocks sincronizados entre producer e consumer (use EC2 na mesma região).
        age = time.time() - message.get("date_created", time.time())

        with self._lock:
            self._metrics.append({
                "processing_time": elapsed,
                "queue_latency":   age,
            })

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self) -> None:
        self.channel.start_consuming()

    # ------------------------------------------------------------------
    # Coleta de métricas — limpa o buffer interno
    # ------------------------------------------------------------------
    def get_data(self) -> list:
        with self._lock:
            data = self._metrics.copy()
            self._metrics.clear()
        return data

    # ------------------------------------------------------------------
    # Processamento simulado
    # ------------------------------------------------------------------
    def process_message(self, data: dict) -> None:
        level = data.get("task_level", 1)
        if level == 1:
            time.sleep(0.001)
        elif level == 2:
            time.sleep(0.01)
        elif level == 3:
            time.sleep(0.01)

    # ------------------------------------------------------------------
    # Parada segura
    # ------------------------------------------------------------------
    def stop(self) -> None:
        print(f"[Consumer {self.id}] Solicitando parada segura...")
        if self.connection and self.connection.is_open:
            self.connection.add_callback_threadsafe(self._realizar_parada_segura)

    def _realizar_parada_segura(self) -> None:
        print(f"[Consumer {self.id}] Encerrando canal e conexão...")
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()


if __name__ == "__main__":
    c = Consumer(id=1)
    c.start_consuming()