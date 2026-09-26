import faust
from models import ChatMessage, BlockedUserEvent


BOOTSTRAP_SERVERS = (
    "kafka-0:9092,"
    "kafka-1:9092,"
    "kafka-2:9092"
)


# Конфигурация Faust-приложения
app = faust.App(
   "TextCensor",
   broker=BOOTSTRAP_SERVERS,
   store="rocksdb://", # Таблицу храним на дисках
)


# Определение топика для входных данных
input_topic = app.topic("messages", value_type=ChatMessage)


# Определение топика для выходных данных
output_topic = app.topic("filtered_messages", value_type=ChatMessage)


# Топик для информации по заблокированным user_id
blocked_users_topic = app.topic("blocked_users", value_type=BlockedUserEvent)


# Таблица для хранения состояний заблокированных пользователей
# Ключ - user_id, кто блокирует
# Значение - множество user_id, кого заблокировал пользователь
blocked_users_table = app.Table(
    "blocked_users",
    partitions=3,    # Количество партиций
    default=set,     # Функция или тип для пропущенных ключей
)


# Множество для хранения запрещенных слов. Храню не в таблице, потому что в задании явно об этом не сказано
blocked_words = set()


# Функция цензурирования сообщений
def censor_messages(message):
    censor_message = ""
    for word in message.split(" "):
        if word in blocked_words:
            censor_message += "*" * len(word)
        else:
            censor_message += word
        censor_message += " "

    return censor_message.rstrip() # убрать последний пробел


# Эндпоинт для добавления или удаления заблокированных слов
# change_type может быть только 'add' или 'del'
@app.page('/blocked_words/{change_type}/{word}')
async def set_blocked_words(web, request, change_type, word):
    try:
        change_type = str(change_type)
        word = str(word)

        # Ошибка, если change_type не add или del
        if change_type != 'add' and change_type != 'del':
           raise ValueError("Invalid change_type value")

        # Удаляем заблокированное слово
        if change_type == 'del' and word in blocked_words:
            blocked_words.remove(word)

        # Добавляем заблокированное слово
        if change_type == 'add' and word not in blocked_words:
            blocked_words.add(word)

        # Возвращаем информацию об успехе
        return web.json({
            'word': word,
            'change_type': change_type,
            'status': "success",
        }, status=200)
    except Exception as e:
        # Возвращаем информацию об ошибке
        return web.json({
            'error': str(e),
            'change_type': change_type,
            'word': word
        }, status=500)


# Агент - сетает блокировки/разблокировки (берет данные из топика blocked_users)
@app.agent(blocked_users_topic)
async def set_blocked_users(Events):
    async for event in Events:
        blocked_users = blocked_users_table.get(event.user_id_from)
        if blocked_users is None:
            blocked_users = set()
        if event.blocked_status is True:
            blocked_users.add(event.user_id_to)
        if event.blocked_status is False:
            blocked_users.remove(event.user_id_to)
        blocked_users_table[event.user_id_from] = blocked_users


# Агент - проверяет блокировки пользователей при входящем сообщении в input_topic
# Цензурирует сообщения и отправляет в выходящий топик
@app.agent(input_topic)
async def filter_blocked_users_and_censor_messages(ChatMessages):
    async for chatMessage in ChatMessages:
        # Логика блокировки отправления текста от заблокированного пользователя
        # если отправитель в множестве заблокированных user_id получателя, то не отправляем
        if chatMessage.user_id_from in blocked_users_table[chatMessage.user_id_to]:
            continue

        chatMessage.message = censor_messages(chatMessage.message)
        await output_topic.send(value=chatMessage) # отправить результат в выходящий топик
