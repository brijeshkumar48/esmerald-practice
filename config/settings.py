from typing import Dict
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv()


class Settings(BaseSettings):
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    # mongo_db: str = os.getenv("MONGO_DB", "test_database")
    media_url: str = os.getenv("MEDIA_PATH", "http://127.0.0.1:8000/media/")
    query_param_operators: Dict[str, str] = Field(
        default_factory=lambda: {
            "eq": "$eq",
            "ne": "$ne",
            "gt": "$gt",
            "gte": "$gte",
            "lt": "$lt",
            "lte": "$lte",
            "in": "$in",
            "nin": "$nin",
            "regex": "$regex",
            "ieq": "ieq",
            "be": "between",
            "sw": "sw",
            "ew": "ew",
        }
    )


settings = Settings()
