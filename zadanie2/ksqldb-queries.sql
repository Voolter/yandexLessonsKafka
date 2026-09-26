-- поток в ksqlDB (messages_stream), который будет принимать входящие сообщения из Kafka.
CREATE STREAM messages_stream (
    user_id STRING,
    recipient_id STRING,
    message STRING,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='messages',   -- Имя топика Kafka
    VALUE_FORMAT='JSON',      -- Формат данных (JSON, AVRO, DELIMITED, и т.д.)
    PARTITIONS=3              -- Число партиций потока (по умолчанию 1)
);

-- таблицы, подсчитывающие:
-- общее количество отправленных сообщений;
CREATE TABLE all_messages AS
    SELECT 'all' as key, COUNT(message) as count -- добавил первую колонку, потому что просил непустой group by
    FROM messages_stream
    group by 'all'
EMIT CHANGES;

-- количество уникальных получателей сообщений;
CREATE TABLE unique_recipients AS
    SELECT 'unique' as key, COUNT_DISTINCT(recipient_id) as count -- добавил первую колонку, потому что просил непустой group by
    FROM messages_stream
    group by 'unique'
EMIT CHANGES;

-- таблица user_statistics для агрегирования данных по каждому пользователю
-- сообщения, отправленные каждым пользователем:
-- количество уникальных получателей для каждого пользователя;
CREATE TABLE user_statistics AS
    SELECT user_id, COUNT(*) AS messages_count, COUNT_DISTINCT(recipient_id) AS unique_recipients_count
    FROM messages_stream
    GROUP BY user_id
EMIT CHANGES;