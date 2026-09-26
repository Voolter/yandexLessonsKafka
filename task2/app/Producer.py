import time
import logging

from confluent_kafka import Producer
from confluent_kafka.serialization import Serializer


logging.basicConfig(
    level=logging.INFO,
    filename="/app/logs/Producer.log", # Логи будут записываться в этот файл
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


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
    try:
        data=serializer(client)
    except Exception as e:
        logger.error(f"Ошибка в сереализации: {e}")
        continue

    producer.produce(
        "my_topic",
        #key=str(clientId), -- убрал для случайного распределения по партициям
        value=data
    )
    # Ожидание завершения отправки всех сообщений
    producer.flush()
    time.sleep(15)

