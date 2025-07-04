Стажировка

---

# Задание 1 Работа с PostgreSQL:

1. Создайте функцию логирования изменений по трем полям.

2. Создайте trigger на таблицу users.

3. Установите расширение pg_cron.

4. Создайте функцию, которая будет доставать только свежие данные (за сегодняшний день) и будет сохранять их в образе Docker по пути /tmp/users_audit_export_, а далее указываете ту дату, за который этот csv был создан.

5. Установите планировщик pg_cron на 3:00 ночи

Решение:

<a href="https://github.com/KG7777/stepik_di_st/blob/main/4.2.sql">Ссылка</a>

---

# Задание 2 Работа с Clickhouse

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

Решение: 

<a href="https://github.com/KG7777/stepik_di_st/blob/main/4.3.sql">Ссылка</a>


---

### :hammer_and_wrench: Languages and Tools :

<div>
  <img src="https://github.com/devicons/devicon/blob/master/icons/git/git-original-wordmark.svg" title="Git" **alt="Git" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/vscode/vscode-original.svg" title="Vscode" **alt="Vscode" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/postgresql/postgresql-original.svg" title="postgresql" **alt="postgresql" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/apachekafka/apachekafka-original.svg" title="kafka" **alt="kafka" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/python/python-original.svg" title="python" **alt="python" width="40" height="40"/>
  <img src="https://github.com/devicons/devicon/blob/master/icons/dbeaver/dbeaver-original.svg" title="dbeaver" **alt="dbeaver" width="40" height="40"/>
</div>



