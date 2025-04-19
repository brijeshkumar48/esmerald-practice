from mongoz import Registry
from config.settings import settings


registry = Registry(url=settings.mongo_uri)
