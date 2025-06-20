
---------------------------------------------------------
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

--------1. Создайте функцию логирования изменений по трем полям.
CREATE OR REPLACE FUNCTION log_user_update()
RETURNS TRIGGER AS $$
DECLARE name_st text;
BEGIN
IF OLD.name IS DISTINCT FROM NEW.name then 
	name_st := 'name';
	INSERT INTO users_audit(user_id,changed_by,field_changed,old_value, new_value)
    VALUES (OLD.id,current_user,name_st,OLD.name, NEW.name);
END IF;
IF OLD.email IS DISTINCT FROM NEW.email then 
	name_st := 'email';
	INSERT INTO users_audit(user_id,changed_by,field_changed,old_value, new_value)
    VALUES (OLD.id,current_user,name_st,OLD.email, NEW.email);
END IF;
IF OLD.role IS DISTINCT FROM NEW.role then 
	name_st := 'role';
	INSERT INTO users_audit(user_id,changed_by,field_changed,old_value, new_value)
    VALUES (OLD.id,current_user,name_st,OLD.role, NEW.role);
END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

--------2. Создайте trigger на таблицу users.

CREATE OR REPLACE TRIGGER trigger_log_user_update
BEFORE UPDATE OF name, email, role ON users  -- Добавлен столбец credit_limit
FOR EACH ROW
EXECUTE FUNCTION log_user_update();



insert into users(name,email,role)
values 
('IVAN','ivan@ya.ru','user'),
('OLEG','oleg@ya.ru','admin'),
('ARINA','arina@ya.ru','user')

update users u 
set 
email = 'oleg_new@ya.ru'
--role='user' 
--name = 'IVANOV IVAN'
where name = 'OLEG'

---------3. Установите расширение pg_cron.
CREATE EXTENSION IF NOT EXISTS pg_cron;

---4. Создайте функцию, которая будет доставать только свежие данные (за сегодняшний день) 
----и будет сохранять их в образе Docker по пути /tmp/users_audit_export_, а далее указываете ту дату, 
----за который этот csv был создан.
CREATE OR REPLACE FUNCTION export_daily_audit()
RETURNS TEXT AS $$
DECLARE
  export_path TEXT;
BEGIN
  export_path := '/tmp/users_audit_export_' || to_char(current_date, 'YYYY-MM-DD') || '.csv';
  
  EXECUTE format('
    COPY (SELECT * FROM users_audit WHERE changed_at::date = current_date) 
    TO %L WITH CSV HEADER', 
    export_path);
  
  RETURN export_path;
END;
$$ LANGUAGE plpgsql;

---5. Установите планировщик pg_cron на 3:00 ночи.
SELECT cron.schedule(
  'export-audit', 
  '0 3 * * *', 
  'SELECT export_daily_audit()'
);

SELECT * FROM cron.job;

