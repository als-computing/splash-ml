from fastapi.testclient import TestClient
from mongomock import MongoClient
import pytest

# from ..tag_service import 
from ..api import app

db = MongoClient().tagdb
tag_svc = TagService(db)

@pytest.fixture
def mongodb():
    return db


@pytest.fixture
def rest_client(mongodb):
    return TestClient(app)
