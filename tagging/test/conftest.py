from fastapi.testclient import TestClient
import pytest
import mongomock

from tagging.api import app, set_tag_service
from tagging.tag_service import TagService
from tagging.graphql import set_gql_tag_service


@pytest.fixture(scope="module")
def mongodb():
    return mongomock.MongoClient().db


@pytest.fixture(scope="module")
def tag_svc(mongodb):
    tag_svc = TagService(mongodb)
    return tag_svc


@pytest.fixture(scope="module")
def rest_client(tag_svc):
    set_tag_service(tag_svc)
    set_gql_tag_service(tag_svc)
    return TestClient(app)
