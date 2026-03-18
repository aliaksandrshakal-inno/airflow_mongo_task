from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import pandas as pd
import pymongo

# датасет для отслеживания изменений в обработанных данных. Когда файл processed_tiktok_data.csv будет обновлен, это будет сигналом для запуска DAG, который загрузит эти данные в MongoDB. Это позволяет нам автоматизировать процесс загрузки данных в базу данных сразу после их обработки, без необходимости вручную запускать DAG.
PROCESSED_DATA_SET = Dataset('/opt/airflow/data/processed_tiktok_data.csv')

@dag(
    dag_id='load_to_mongo_dag',
    schedule=[PROCESSED_DATA_SET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['homework', 'mongodb']
)
def load_to_mongo():

    @task
    def upload_to_mongodb():
        df = pd.read_csv('/opt/airflow/data/processed_tiktok_data.csv')
        
        # подключение к MongoDB. Здесь мы используем pymongo для подключения к MongoDB, которая запущена в контейнере с именем "mongodb". Мы указываем имя пользователя, пароль и адрес сервера. Затем мы выбираем базу данных "task_db" и коллекцию "reviews", куда будем загружать данные.
        client = pymongo.MongoClient("mongodb://admin:password@mongodb:27017/")
        db = client.task_db
        collection = db.reviews
        
        # загрузка данных в MongoDB. Перед загрузкой мы очищаем коллекцию от старых данных, чтобы избежать дублирования. Затем мы конвертируем DataFrame в список словарей и используем insert_many для загрузки всех записей в коллекцию. После успешной загрузки мы выводим сообщение с количеством загруженных записей.
        collection.delete_many({}) 
        records = df.to_dict('records')
        collection.insert_many(records)
        print(f"Successfully uploaded {len(records)} records to MongoDB")

    upload_to_mongodb()

load_to_mongo()