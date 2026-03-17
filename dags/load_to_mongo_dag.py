from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import pandas as pd
import pymongo # Используем прямой драйвер вместо MongoHook

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
        
        # Прямое подключение через pymongo
        # Параметры берем те же: admin/password и имя сервиса mongodb
        client = pymongo.MongoClient("mongodb://admin:password@mongodb:27017/")
        db = client.tiktok_db
        collection = db.reviews
        
        collection.delete_many({}) 
        records = df.to_dict('records')
        collection.insert_many(records)
        print(f"Successfully uploaded {len(records)} records to MongoDB")

    upload_to_mongodb()

load_to_mongo()