--1.Поддерживать сырые логи событий (схема выше). Обратите внимание на то, что данные в этой таблице должны хранится 30 дней.
CREATE TABLE user_events(
	user_id UInt32,
	event_type String,
	points_spent UInt32,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time,user_id)
TTL event_time + INTERVAL 30 DAY

--2.Построить агрегированную таблицу. Храним агрегаты 180 дней, чтобы делать трендовый анализ:
--уникальные пользователи по event_type и event_date
--сумма потраченных баллов
--количество действий

CREATE TABLE user_agg (
    event_date Date,
    event_type String,
    uniq_users AggregateFunction(uniq, UInt32),
    sum_points_spent AggregateFunction(sum, UInt32),
    event_count AggregateFunction(count)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date,event_type)
TTL event_date + INTERVAL 180 DAY

--3.Сделать Materialized View, которая:
--при вставке данных в таблицу сырых логов событий, будет обновлять агрегированную таблицу
--использует sumState, uniqState, countState


CREATE MATERIALIZED VIEW user_events_mv
TO user_agg
AS
SELECT 
	toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS uniq_users,
    sumState(points_spent) AS sum_points_spent,
    countState() AS event_count
FROM user_events
GROUP BY event_date,event_type;

--DROP TABLE default.user_agg

INSERT INTO user_events VALUES
-- События 10 дней назад
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());S

--4.Создать запрос, показывающий:
--Retention: сколько пользователей вернулись в течение следующих 7 дней. 
--Как считается Retention? Гуглим. Формат результата - total_users_day_0|returned_in_7_days|retention_7d_percent|

With first_t as (
Select
	distinct
	user_id,
	toDate(event_time) AS active_date,
	min(toDate(event_time)) OVER (PARTITION BY user_id) AS first_day
from
	user_events ue),
retention_data as (
Select
	first_day,
	COUNT(DISTINCT user_id) AS total_users_day_0,
	COUNT(distinct case
		WHEN active_date BETWEEN first_day + 1 AND first_day + 7 
		then user_id 
	end) AS returned_in_7_days
from
		first_t
group by
	first_day)
SELECT
	total_users_day_0,
	returned_in_7_days,
	ROUND(returned_in_7_days * 100.0 / NULLIF(total_users_day_0, 0), 2) AS retention_7d_percent
FROM
	retention_data
ORDER BY
	first_day DESC;
       
--5.Создать запрос с группировками по быстрой аналитике по дням, формат ниже.    
SELECT 
	event_date,
    event_type, 
    uniqMerge(uniq_users) uniq_users ,
    sumMerge(sum_points_spent) total_spent,
    countMerge(event_count) total_actions
    FROM user_agg
    group by 
    event_date,
    event_type 
order BY event_date,event_type  

