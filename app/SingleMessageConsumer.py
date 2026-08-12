import logging

from confluent_kafka import Consumer
from confluent_kafka.serialization import Deserializer


logging.basicConfig(
    level=logging.INFO,
    filename="/app/logs/SingleMessageConsumer.log", # Логи будут записываться в этот файл
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class Client:
    def __init__(self, clientId: str, name: str):
        self.clientId = clientId
        self.name = name


class ClientDeserializer(Deserializer):
    def __call__(self, value: bytes):
       if value is None:
           return None

       name_size = int.from_bytes(value[0:4], byteorder="big")
       name_bytes = value[4:4 + name_size]
       name = name_bytes.decode("utf-8")

       id_bytes = value[4 + name_size:8 + name_size]
       id_value = id_bytes.decode("utf-8")

       return Client(id_value, name)


# Подключение к одному из предстваленных трех контейнеров (Для отказоустойчивости)
BOOTSTRAP_SERVERS = (
    "kafka-0:9092,"
    "kafka-1:9092,"
    "kafka-2:9092"
)


# Настройка консьюмера
conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS, # Адрес брокера Kafka
    "group.id": "SingleGroup",              # Уникальный идентификатор группы
    "auto.offset.reset": "latest",          # Начало чтения с конца (чтобы только новые сообщения читать)
    "enable.auto.commit": True,             # Автоматический коммит смещений
    "session.timeout.ms": 6_000,            # Время ожидания активности от консьюмера
}
consumer = Consumer(conf)
consumer.subscribe(["my_topic"])


deserializer = ClientDeserializer()


try:
    while True:
        msg = consumer.poll(0.1)

        if msg is None:
            continue
        if msg.error():
            logger.error(f"Ошибка: {msg.error()}")
            continue

        try:
            value = deserializer(msg.value())
        except Exception as e:
            logger.error(f"Ошибка в десериализации: {e}")
            continue
        logger.info(f"partition={msg.partition()}, offset={msg.offset()}, clientId={value.clientId}, name={value.name}")
finally:
    consumer.close()