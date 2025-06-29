from pymongo import MongoClient
from datetime import date,datetime, timedelta
import json
import os

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]

#db["archived_users"].drop()
#db.create_collection("archived_users") 

# Текущая дата и дата 30 дней назад
current_date = datetime.now()
start_date = current_date - timedelta(days=30)
active_date = current_date - timedelta(days=14)
arc_users_count = 0

#коллекция для архива
collection_archived = db["archived_users"]

# Получить с фильтром
results = collection.find({"user_info": {"$exists":True}})
start_date = datetime.now() - timedelta(30)
my_list_ids = [] 

#находим пользователей необходимых
for doc in results:
    if doc["user_info"]["registration_date"] < start_date and doc["event_time"] < active_date :   
        arc_users_count += 1
        my_list_ids.append(doc["user_id"]) 
        #копируем нужный элемент коллекции
        doc_to_transfer = doc.copy()

        #вставляем во вторую коллекцию
        collection_archived.insert_one(doc_to_transfer)
        #удаляем из первой коллекции
        result_delete = collection.delete_one({"user_id": doc["user_id"]})
        print(f"Удален: {doc["user_id"]}")
        
#коллекция для отчета
archived_data_file = {
    "date": date.today().isoformat(),
    "archived_users_count": arc_users_count,
    "archived_users_ids": my_list_ids
}

print("\nВсе документы в коллекции archived_users:")
for doc in db.archived_users.find():
    print(doc)

print("\nВсе документы в коллекции user_events:")
for doc in db.user_events.find():
    print(doc)

# Сохраняем в файл
with open(f"{date.today()}.json", "w") as f:
     json.dump(archived_data_file, f, indent=2, default=str)

client.close