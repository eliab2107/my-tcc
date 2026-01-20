import pika
import json
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
channel = connection.channel()

channel.queue_declare(queue="fila_teste")

counter = 0

print("[PUBLISHER] Publicando mensagens...")

while True:
    message = {
        "id": counter,
        "conteudo": f"Mensagem {counter}",
        "cep": "01001-000"
    }

    channel.basic_publish(
        exchange="",
        routing_key="fila_teste",
        body=json.dumps(message).encode("utf-8")
    )

    counter += 1
    #time.sleep()
