import os

from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient

load_dotenv(find_dotenv())


class Config:
    MONGO = os.getenv('MONGO')

    def pymongo_client(self):
        return MongoClient(host=self.MONGO)


cfg = Config()
