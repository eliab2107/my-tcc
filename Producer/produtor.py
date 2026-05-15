#Produtor

from multiprocessing import connection
import time
import pika
import json
from dotenv import load_dotenv
from random import randint, choices, choice
import argparse
import os

load_dotenv()


class Produtor():

    def __init__(self, time_sleep=0.01, mode="skewed", constant_task=1):
        self.time_sleep = float(time_sleep)
        self.mode = mode
        self.constant_task = int(constant_task)

        self.host = os.getenv("RABBITMQ_HOST")
        self.user = os.getenv("RABBITMQ_USER")
        self.password = os.getenv("RABBITMQ_PASS")

        credentials = pika.PlainCredentials(self.user, self.password)

        parametros = pika.ConnectionParameters(
            host=self.host,
            credentials=credentials
        )

        self.connection = pika.BlockingConnection(parametros)
        self.channel = self.connection.channel()
        self.queue = os.getenv("RABBITMQ_QUEUE", "fila_teste")
        self.channel.queue_declare(queue=self.queue)
        self.counter = 0

    def _next_task(self):
        if self.mode == "constant":
            return self.constant_task
        elif self.mode == "skewed":
            # 70% type 1, 20% type 2, 10% type 3
            return choices([1, 2, 3], weights=[70, 20, 10], k=1)[0]
        elif self.mode == "balanced":
            return choice([1, 2, 3])
        # others
        return randint(1, 3)

    def publicar_mensagens(self):
        print(f"[PUBLISHER] Publicando mensagens... mode={self.mode} sleep={self.time_sleep}")

        try:
            while True:
                task = self._next_task()

                message_body = {
                    "conteudo": f"Mensagem {self.counter}",
                    "cep": "01001-000",
                    "date_created": time.time(),
                    "task_level": task,
                }

                self.channel.basic_publish(
                    exchange="",
                    routing_key=self.queue,
                    body=json.dumps(message_body).encode("utf-8"),
                )

                #time.sleep(self.time_sleep)
        except KeyboardInterrupt:
            print("[PUBLISHER] Interrompido pelo usuário, fechando conexão...")
        finally:
            try:
                self.connection.close()
            except Exception:
                pass


def _parse_args():
    parser = argparse.ArgumentParser(description="Produtor de mensagens RabbitMQ com modos de envio")
    parser.add_argument("--mode", choices=["constant", "skewed", "balanced"], default="constant",
                        help="Modo de execução: constant (sempre mesma tarefa), skewed (70/20/10), balanced (igual)")
    parser.add_argument("--task", type=int, choices=[1, 2, 3], default=1,
                        help="Tarefa a enviar quando --mode constant é usada")
    parser.add_argument("--sleep", type=float, default=0.01,
                        help="Tempo de espera (segundos) entre envios")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    produtor = Produtor(time_sleep=args.sleep, mode=args.mode, constant_task=args.task)
    produtor.publicar_mensagens()