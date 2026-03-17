from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
import os
import pandas as pd
import re
from airflow.datasets import Dataset

PROCESSED_DATA_SET = Dataset('/opt/airflow/data/processed_tiktok_data.csv')

# Настройка путей
# Используем переменную окружения или путь по умолчанию
# ВАЖНО: В Docker Airflow всегда живет в /opt/airflow
AIRFLOW_HOME = '/opt/airflow' 
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
FILE_NAME = 'tiktok_google_play_reviews.csv'
RAW_FILE = os.path.join(DATA_DIR, FILE_NAME)
PROCESSED_FILE = os.path.join(DATA_DIR, 'processed_tiktok_data.csv')

@dag(
    dag_id='data_processing_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def data_processing_dag():
    
    # 1. Sensor: Ждет появления файла raw_data.csv
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='fs_default',
        filepath=FILE_NAME,  # <--- Замени строку на переменную FILE_NAME
        poke_interval=10,
        timeout=600
    )

    # 2. Branching: Проверка, не пустой ли файл
    @task.branch(task_id='check_if_file_is_empty')
    def check_file():
        if os.path.exists(RAW_FILE) and os.path.getsize(RAW_FILE) > 0:
            return 'process_data_group.replace_nulls'
        else:
            return 'log_empty_file'

    # 3.1 Ветка для пустого файла: BashOperator
    log_empty_file = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "ERROR: The file is empty. Processing aborted."'
    )

    # 3.2 Ветка обработки: TaskGroup
# 3.2 Ветка обработки: TaskGroup
    with TaskGroup(group_id='process_data_group') as process_data_group:
        
        @task(task_id='replace_nulls')
        def replace_nulls():
            df = pd.read_csv(RAW_FILE)
            # Заменяем пустые значения (null) во всем файле
            df = df.fillna('-')
            df.to_csv(PROCESSED_FILE, index=False)
            return PROCESSED_FILE

        @task(task_id='sort_by_date')
        def sort_by_date(file_path):
            df = pd.read_csv(file_path)
            # ВНИМАНИЕ: используем колонку 'at', как на твоем скриншоте
            if 'at' in df.columns:
                df['at'] = pd.to_datetime(df['at'])
                df = df.sort_values(by='at')
            df.to_csv(file_path, index=False)
            return file_path

        @task(task_id='clean_content',outlets=[PROCESSED_DATA_SET])
        def clean_content(file_path):
            df = pd.read_csv(file_path)
            if 'content' in df.columns:
                # Удаляем смайлики и странные символы из отзывов
                df['content'] = df['content'].apply(lambda x: re.sub(r'[^\w\s\d.,!?;:-]', '', str(x)))
            df.to_csv(file_path, index=False)

        # Соединяем задачи
        clean_content(sort_by_date(replace_nulls()))

    # 4. Основные зависимости
    wait_for_file >> check_file() >> [process_data_group, log_empty_file]

data_processing_dag()