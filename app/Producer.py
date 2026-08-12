import time

from confluent_kafka import Producer
from confluent_kafka.serialization import Serializer


class Client:
    def __init__(self, clientId: str, name: str):
        self.clientId = clientId
        self.name = name


class ClientSerializer(Serializer):
    def __call__(self, obj: Client):
       name_bytes = obj.name.encode("utf-8")
       name_size = len(name_bytes)

       result = name_size.to_bytes(4, byteorder="big")
       result += name_bytes
       result += obj.clientId.encode("utf-8")
       return result


# Подключение к одному из предстваленных трех контейнеров (Для отказоустойчивости)
BOOTSTRAP_SERVERS = (
    "kafka-0:9092,"
    "kafka-1:9092,"
    "kafka-2:9092"
)


# Примерные данные, которые будут поставляться в топик
client = Client(
    clientId="123",
    name="Alexey"
)
serializer = ClientSerializer()


# Инициализация продюсера
producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
})


# Отправка данных в бесконечным цикле с паузой
while True:
    producer.produce(
        "my_topic",
        #key=str(clientId), -- убрал для случайного распределения по партициям
        value=serializer(client)
    )
    # Ожидание завершения отправки всех сообщений
    producer.flush()
    time.sleep(15)

