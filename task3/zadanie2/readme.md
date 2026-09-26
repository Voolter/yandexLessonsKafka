Запуск реализуется с помощью docker-compose up -d.<br>
Моя последовательность действий была следующей:<br>

```
#1 Поднял кафка кластер и сервисы ksqldb с помощью docker-compose up -d на kraft
#2 Посмотрел статус и имя контейнеров docker ps -a
#3 Вошел в shell среду случайного контейнера docker exec -it <...> /bin/sh
#4 Создал топики (команды в topic.txt)
```

Затем выполнил через ui sql из ksqldb-queries.sql

```
Затем попробовал отправить в топик messages сообщение:
{
    "user_id": "1",
    "recipient_id": "2",
    "message": "this is a test",
    "timestamp": 1790436524
}

{
    "user_id": "2",
    "recipient_id": "3",
    "message": "this is a test",
    "timestamp": 1790436525
}

{
    "user_id": "3",
    "recipient_id": "4",
    "message": "this is a test",
    "timestamp": 1790436526
}
```


тестовые запросы в ksqlDB для проверки:
общего количества отправленных сообщений;
числа уникальных получателей:
```
select * from all_messages;

select * from unique_recipients;
```


Проверить работоспособность приложения можно, выполнив docker-compose up -d схожим образом, как сделал я