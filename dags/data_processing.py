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

# для удобства работы с файлами определим константы для путей и имен файлов. Это поможет избежать ошибок и сделает код более читаемым.
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
    # сеnsor для отслеживания появления файла. Он будет проверять наличие файла каждые 10 секунд в течение 10 минут. Если файл не появится, задача завершится с ошибкой.
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='fs_default',
        filepath=FILE_NAME,
        poke_interval=10,
        timeout=600
    )

    # ветка для проверки наличия данных в файле. Если файл существует и не пустой, мы переходим к обработке данных. Если файл пустой, мы логируем ошибку и останавливаем дальнейшую обработку. Это важно для предотвращения ошибок при попытке обработки пустого файла.
    @task.branch(task_id='check_if_file_is_empty')
    def check_file():
        if os.path.exists(RAW_FILE) and os.path.getsize(RAW_FILE) > 0:
            return 'process_data_group.replace_nulls'
        else:
            return 'log_empty_file'

    log_empty_file = BashOperator(
        task_id='log_empty_file',
        bash_command='echo "ERROR: The file is empty. Processing aborted."'
    )

    # группа задач для обработки данных. Внутри нее мы выполняем несколько шагов: замена null-значений, сортировка по дате и очистка текста от нежелательных символов. Каждый шаг оформлен как отдельная задача, что позволяет легко отслеживать процесс и при необходимости вносить изменения в конкретные этапы обработки.
    with TaskGroup(group_id='process_data_group') as process_data_group:
        
        @task(task_id='replace_nulls')
        def replace_nulls():
            df = pd.read_csv(RAW_FILE)
            df = df.fillna('-')
            df.to_csv(PROCESSED_FILE, index=False)
            return PROCESSED_FILE

        @task(task_id='sort_by_date')
        def sort_by_date(file_path):
            df = pd.read_csv(file_path)
            if 'at' in df.columns:
                df['at'] = pd.to_datetime(df['at'])
                df = df.sort_values(by='at')
            df.to_csv(file_path, index=False)
            return file_path

        @task(task_id='clean_content',outlets=[PROCESSED_DATA_SET])
        def clean_content(file_path):
            df = pd.read_csv(file_path)
            if 'content' in df.columns:
                df['content'] = df['content'].apply(lambda x: re.sub(r'[^\w\s\d.,!?;:-]', '', str(x)))
            df.to_csv(file_path, index=False)

        clean_content(sort_by_date(replace_nulls()))

    wait_for_file >> check_file() >> [process_data_group, log_empty_file]

data_processing_dag()