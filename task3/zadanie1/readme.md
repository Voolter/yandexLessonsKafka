Классы в приложении:
```
ChatMessage,
BlockedUserEvent
```

Логика приложения:
```
Существуют три топика, в которые отправляются сообщения
В топики messages и blocked_users сообщения отправляются из ui
В топик filtered_messages сообщения отправляет stream_processor

в blocked_users отправляется информация о блокировках пользователей, stream_processor хранит это состояние в таблице.
Также реализована логика цензурирования сообщений.
Заблокированные слова динамически добавляются или удаляются через эндпоинт.
```

Запуск реализуется с помощью docker-compose up -d.<br>
Моя последовательность действий была следующей:<br>

```
#1 Поднял кафка кластер с помощью docker-compose up -d на kraft
#2 Посмотрел статус и имя контейнеров docker ps -a
#3 Вошел в shell среду случайного контейнера docker exec -it 0eececf91b4c /bin/sh
#4 Создал топики (команды в topic.txt)
#5 Добавил сервис stream_processor и опять выполнил docker-compose up -d
```

```
с помощью curl http://localhost:6066/blocked\_words/add/test
внес слово 'test' в блокировку

по логам увидел успешный результат:
% curl http://localhost:6066/blocked\_words/add/test
{"word":"test","change_type":"add","status":"success"}%                                                       
```


```
Затем попробовал отправить в топик messages сообщение:
{
    "user_id_from": "1",
    "user_id_to": "2",
    "message": "this is a test"
}

Результат в топике filtered_messages получился ожидаемым:
{
	"user_id_from": "1",
	"user_id_to": "2",
	"message": "this is a ****",
	"__faust": {
		"ns": "stream_processor.ChatMessage"
	}
}

Затем отправил сообщение о блокировке пользователем 2 пользователя 1 в топик blocked_users
{
    "user_id_from": "2",
    "user_id_to": "1",
    "blocked_status": true
}

и затем отправил на проверку сообщение в топик messages:
{
    "user_id_from": "1",
    "user_id_to": "2",
    "message": "this is a test"
}

и, ожидаемо, не получил это сообщение в топик filtered_messages
```



Проверить работоспособность приложения можно, выполнив docker-compose up -d схожим образом, как сделал я