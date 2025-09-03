:woman_technologist: Стажировка

---

# Задание 2 Работа с PostgreSQL:

1. Создайте функцию логирования изменений по трем полям.

2. Создайте trigger на таблицу users.

3. Установите расширение pg_cron.

4. Создайте функцию, которая будет доставать только свежие данные (за сегодняшний день) и будет сохранять их в образе Docker по пути /tmp/users_audit_export_, а далее указываете ту дату, за который этот csv был создан.

5. Установите планировщик pg_cron на 3:00 ночи

## :white_check_mark: Решение:

<a href="https://github.com/KG7777/stepik_di_st/blob/main/4.2.sql">Ссылка</a>

---

# Задание 3 Работа с Clickhouse

У Вас есть события пользователя (user_events), которые записываются в Clickhouse.

<img src="https://ucarecdn.com/7c14f118-54b5-4840-b8a9-1f10629aa2d1/">

1. Поддерживать сырые логи событий (схема выше). Обратите внимание на то, что данные в этой таблице должны хранится 30 дней.

2. Построить агрегированную таблицу. Храним агрегаты 180 дней, чтобы делать трендовый анализ:

-уникальные пользователи по event_type и event_date

-сумма потраченных баллов

-количество действий

3. Сделать Materialized View, которая:

-при вставке данных в таблицу сырых логов событий, будет обновлять агрегированную таблицу

-использует sumState, uniqState, countState

4. Создать запрос, показывающий:
Retention: сколько пользователей вернулись в течение следующих 7 дней. Как считается Retention? Гуглим. Формат результата - total_users_day_0|returned_in_7_days|retention_7d_percent|

5. Создать запрос с группировками по быстрой аналитике по дням, формат ниже.

<img src="https://ucarecdn.com/e0f99443-1a80-4676-9a70-1cef087af415/">

## :white_check_mark: Решение: 

<a href="https://github.com/KG7777/stepik_di_st/blob/main/4.3.sql">Ссылка</a>

# Задание 4 Работа с NoSQL

В базе данных MongoDB хранятся события пользователей в коллекции user_events. Каждый документ содержит информацию о пользователе и его действиях.

1. Ежедневно находить пользователей, которые:

- Зарегистрировались более 30 дней назад

- И не проявляли активности последние 14 дней

3. Перемещать этих пользователей в архивную коллекцию archived_users

4. Сохранять отчёт в формате .json о количестве пользователей, архивированных в этот день. Автоматизация через cron не нужна, запуск скрипта - ручной, ежедневный. Используйте только pymongo, json, os и datetime. Формат отчета - 
<img src="https://ucarecdn.com/980fee3f-71d9-479d-a9bd-ac126b42f488/">

## :white_check_mark: Решение: 

<a href="https://github.com/KG7777/stepik_di_st/blob/main/4.6.sql">Ссылка</a>

# Задание 5 Работа с Kafka

В базе данных PostgreSQL хранится таблица user_logins. В ней содержатся события пользователей, такие как логин, регистрация, покупка и т.д. Каждый раз, когда необходимо перенести эти события из PostgreSQL в другую систему (например, ClickHouse), можно воспользоваться Kafka как промежуточным звеном для передачи сообщений. Однако, в реальных задачах возникает риск повторной отправки уже обработанных данных. Чтобы избежать дублирования, нужно использовать дополнительное логическое поле в таблице — sent_to_kafka BOOLEAN, которое будет сигнализировать, были ли данные уже отправлены в Kafka.

Задание проверяется по следующим критериям:

 - Работает продюсер: он не отправляет повторно записи и корректно выставляет флаг sent_to_kafka

 - Работает консьюмер: получает данные из Kafka и сохраняет в ClickHouse

 - README содержит описание пайплайна и как его запустить

В результате реализации получится устойчивое решение миграции данных с защитой от дубликатов.

## :white_check_mark: Решение: 

<a href="https://github.com/KG7777/stepik_di_st/blob/main/5.2/README.md">Ссылка</a>

# Задание 6 Работа с объектными хранилищами(Selectel)

Первое задание:

 - Добавьте метод list_files(), который будет возвращать список объектов в бакете.
 - Добавьте метод file_exists(), который должен возвращать булевый ответ на запрос о наличии файла с определенным именем.
Критерии приема: методы корректно принимают параметры, возвращают ответ, код является рабочим (скриншот/запись экрана/передача credentials для подключения).

## :white_check_mark: Решение: 

<a href="https://github.com/KG7777/stepik_di_st/blob/main/6.4/selectel.py">Ссылка</a>

Для отображения процесса:

<img src="https://github.com/KG7777/stepik_di_st/blob/main/6.4/Отчет1.1.png">

Второе задание:

Для отображение процесса

# Задание 8 Реализация генератора DAGs

Реализация есть только по SQL. Но хочется хоть что-то сдать...

1. Создается даг, который при запуске сгенерирует другой даг по шаблону
<img src="https://github.com/KG7777/stepik_di_st/blob/main/8.5/1.png">
<img src="https://github.com/KG7777/stepik_di_st/blob/main/8.5/1_1.png">
2. Должны быть три папки: json и templates и scripts(Для python функции)
<img src="https://github.com/KG7777/stepik_di_st/blob/main/8.5/2_json.png">
<img src="https://github.com/KG7777/stepik_di_st/blob/main/8.5/2_templates.png">
3. Json заполняется по шаблону: 
<a href="https://github.com/KG7777/stepik_di_st/blob/main/8.5/sql.py">Ссылка SQL json</a>
4. Для SQL 4 типа SELECT INSERT UPDATE DELETE
<a href="https://github.com/KG7777/stepik_di_st/blob/main/8.5/python.py">Ссылка python json</a>
5. Для python пока два вида это API_CALL и VALIDATION
4. После выполнения дага, файлы отработанные переносятся в папку archive
<img src="https://github.com/KG7777/stepik_di_st/blob/main/8.5/archive.png">
5. Для sql создается даг example_dag_name
<img src="https://github.com/KG7777/stepik_di_st/blob/main/8.5/sql.png">
6. Для python создается даг validation_api_dag


---

### :hammer_and_wrench: Languages and Tools :

<div>
  <img src="https://github.com/devicons/devicon/blob/master/icons/git/git-original.svg" title="Git" **alt="Git" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/vscode/vscode-original.svg" title="Vscode" **alt="Vscode" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/postgresql/postgresql-original.svg" title="postgresql" **alt="postgresql" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/apachekafka/apachekafka-original.svg" title="kafka" **alt="kafka" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/python/python-original.svg" title="python" **alt="python" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/dbeaver/dbeaver-original.svg" title="dbeaver" **alt="dbeaver" width="40" height="40"/>
  <img src="clickhouse.svg" title="clickhouse" **alt="clickhouse" width="40" height="40"/>
</div> 



