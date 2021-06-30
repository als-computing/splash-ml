from fastapi.testclient import TestClient
from mongomock import MongoClient
import pytest

from ..api import (
    app,
    API_URL_PREFIX,
    svc_context)

from ..model import (
    Dataset,
    LABEL_NAME,
    Tag,
    TagSource
)
from ..tag_service import TagService


db = MongoClient().tagdb
tag_svc = TagService(db)
svc_context.tag_svc = tag_svc

@pytest.fixture
def mongodb():
    return db


@pytest.fixture
def rest_client(mongodb):
    return TestClient(app)


def test_taggers(rest_client: TestClient):

    response = rest_client.post(API_URL_PREFIX + "/tagsources", json=tag_source_1_dict)
    response = rest_client.post(API_URL_PREFIX + "/tagsources", json=tag_source_2_dict)
    response: TagSource = rest_client.get(API_URL_PREFIX + "/tagsources")
    tag_sources = response.json()
    assert len(tag_sources) == 2, "made two tag sources"


def test_tags_and_assets(rest_client: TestClient):
    # create a dataset
    response = rest_client.post(API_URL_PREFIX + "/datasets", json=dataset)
    assert response.status_code == 200

    # search on name
    response: Dataset = rest_client.get(
        API_URL_PREFIX + "/datasets",
        params={"skip": 0, "limit": 10, "type": "file", "uri": "/foo/bar.h5"})
    assert response.status_code == 200, f"oops {response.text}"
    tag_sources = response.json()
    assert len(tag_sources) == 1

    # add a new tag
    new_tag = Tag(name=LABEL_NAME, value="peaks", confidence=0.1)
    response = rest_client.post(
        f"{API_URL_PREFIX}/datasets/{tag_sources[0]['uid']}/tags",
        json=[new_tag.dict()])
    assert response.status_code == 200, f"oops {response.text}"

    # find the asset based on tags
    response: Dataset = rest_client.get(
        API_URL_PREFIX + "/datasets",
        params={"skip": 0, "limit": 10, "tag_value": "peaks"})
    assert response.status_code == 200, f"oops {response.text}"
    tag_sources = response.json()
    assert len(tag_sources) == 1


def test_skip_limit(rest_client: TestClient):
    response = rest_client.post(API_URL_PREFIX + "/datasets", json=dataset)
    assert response.status_code == 200

    response = rest_client.post(API_URL_PREFIX + "/datasets", json=dataset2)
    assert response.status_code == 200

    response: Dataset = rest_client.get(
        API_URL_PREFIX + "/datasets",
        params={"skip": 1, "limit": 1})
    assert response.status_code == 200, f"oops {response.text}"
    source = response.json()
    assert len(source) == 1



tag_source_1_dict = {
    "type": "model",
    "name": "deep thought",
    "model_info": {
        "label_index": {
            "red": 0.1,
            "green": 0.2
        }
    }
}

tag_source_2_dict = {
    "type": "human",
    "name": "slartibartfast"
}

dataset = {
    "type": "file",
    "uri": "/foo/bar.h5",
    "tags": [
        {"name": "label", "value": "rods", "confidence": 0.9}
    ]
}

dataset2 = {
    "type": "file",
    "uri": "/foo/doi.h2",
}
