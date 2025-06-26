from pymongo import MongoClient
from datetime import date,datetime, timedelta
import json
import os

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]

#db.create_collection("archived_users")

# Текущая дата и дата 30 дней назад
current_date = datetime.now()
start_date = current_date - timedelta(days=30)
active_date = current_date - timedelta(days=14)
arc_users_count = 0

# Получить с фильтром
results = collection.find({"user_info": {"$exists":True}})
start_date = datetime.now() - timedelta(30)
my_list_ids = [] 
#print(date.today())
for doc in results:
    #print(doc["user_info"]["registration_date"] )
    #print(doc["event_time"])
    if doc["user_info"]["registration_date"] < start_date and doc["event_time"] < active_date :   
        arc_users_count += 1
        my_list_ids.append(doc["user_id"]) 
        
# Преобразуем date в datetime
today_datetime = datetime.combine(date.today(), datetime.min.time())
#print(date.today().isoformat())

#print(arc_users_count)
#print(my_list_ids)

archived_user = {
    "date": date.today().isoformat(),
    "archived_users_count": arc_users_count,
    "archived_users_ids": my_list_ids
}

collection_archived = db["archived_users"]
result_delete = collection_archived.delete_many({"date": archived_user["date"]})
print(f"Удалено кол-во: {result_delete.deleted_count}")

result = db.archived_users.insert_one(archived_user)
print(f"Запись добавлена с датой: {archived_user['date']}")  

print("\nВсе документы в коллекции archived_users:")
for doc in db.archived_users.find():
    print(doc)

# Сохраняем в файл
with open(f"{date.today()}.json", "w") as f:
     json.dump(archived_user, f, indent=2, default=str)
