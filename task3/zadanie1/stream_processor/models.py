import faust


# Определяем модель данных для сообщения
class ChatMessage(faust.Record):
    user_id_from: str
    user_id_to: str
    message: str


# Определяем модель данных для блокировки/разблокировки пользователей
class BlockedUserEvent(faust.Record):
    user_id_from: str
    user_id_to: str
    blocked_status: bool