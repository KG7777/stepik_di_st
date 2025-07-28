import asyncio

from pathlib import Path

import os
from dotenv import load_dotenv

load_dotenv()  # Загружает переменные из .env

# Для создания асинхронного контекстного менеджера
from contextlib import asynccontextmanager

# Асинхронная версия boto3
from aiobotocore.session import get_session

# Ошибки при обращении к API
from botocore.exceptions import ClientError

import logging
from datetime import datetime

#логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('selectel_s3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('selectel_s3')

class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection

    async def send_file(self, local_source: str):
       file_ref = Path(local_source)
       target_name = file_ref.name
       logger.info(f"Uploading file: {local_source} as {target_name}") #логирование
       async with self._connect() as remote:
          with file_ref.open("rb") as binary_data:
             await remote.put_object(
                 Bucket=self._bucket,
                 Key=target_name,
                 Body=binary_data
             )
       logger.info(f"Successfully uploaded: {target_name}")

    async def fetch_file(self, remote_name: str, local_target: str):
        logger.info(f"Downloading file: {remote_name} to {local_target}") 
        async with self._connect() as remote:
            response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
            body = await response["Body"].read()
            with open(local_target, "wb") as out:
                out.write(body)
        logger.info(f"Successfully downloaded: {remote_name}") 

    async def remove_file(self, remote_name: str):
        async with self._connect() as remote:
            await remote.delete_object(Bucket=self._bucket, Key=remote_name)
        logger.info(f"Successfully deleted: {remote_name}")

    # задание 1 часть
    async def list_files(self) -> list[str]:
        """
        Возвращает список всех файлов в бакете
        Returns:
            list[str]: Список имен файлов (ключей) в хранилище
        """
        logger.info("Listing files in bucket") 
        async with self._connect() as remote:
            # Получаем список объектов в бакете
            response = await remote.list_objects_v2(Bucket=self._bucket)
            files = [obj['Key'] for obj in response.get('Contents', [])]
            logger.info(f"Found {len(files)} files in bucket")  
            return files
    
    #задание 1 часть
    async def file_exists(self, remote_name: str) -> bool:
        """
        Проверяет существование файла в хранилище
        
        Args:
            remote_name: Имя файла
            
        Returns:
            bool: True если файл существует, False если нет
        """
        logger.info(f"Checking if file exists: {remote_name}")
        async with self._connect() as remote:
            try:
                # head_object для проверки существования файла
                await remote.head_object(Bucket=self._bucket, Key=remote_name)
                logger.info(f"File exists: {remote_name}") 
                return True
            except ClientError as e:
                # Если получили ошибку 404 (Not Found), файла нет
                if e.response['Error']['Code'] == '404':
                    logger.info(f"File not found: {remote_name}") 
                    return False
                logger.error(f"Error checking file: {str(e)}") 
                # Для других ошибок - пробрасываем исключение дальше
                raise



async def run_demo():
    storage = AsyncObjectStorage(
    key_id=os.getenv("S3_KEY_ID"),
    secret=os.getenv("S3_SECRET"),
    endpoint=os.getenv("S3_ENDPOINT"),
    container=os.getenv("S3_BUCKET")
)
    await storage.send_file("test.txt")

     # Проверяем его существование
    exists = await storage.file_exists("test.txt")
    print(f"Файл test.txt существует в хранилище: {exists}")

    # Получаем список всех файлов
    files = await storage.list_files()
    print(f"Файлы в хранилище: {files}")


    await storage.fetch_file("test.txt", "downloaded_test.txt")
    
    await storage.remove_file("test.txt")

    # Проверяем снова
    exists = await storage.file_exists("test.txt")
    print(f"Файл test.txt существует после удаления: {exists}")


if __name__ == "__main__":
    asyncio.run(run_demo())