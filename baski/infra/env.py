from google.cloud import firestore
from functools import lru_cache

project_id = firestore.Client().project
