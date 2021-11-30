from fastapi.testclient import TestClient
import pytest

from tagging.tag_service import TagService

from .data import (
    json_datetime,
    new_dataset,
    new_tagging_event,
    new_tagger,
    no_tag_dataset,
    no_tag
)


@pytest.fixture
def load_datasets(tag_svc: TagService):
    tag_svc.create_dataset(new_dataset)


@pytest.mark.usefixtures("load_datasets")
def test_query_uri(rest_client: TestClient):
    response = rest_client.post("/graphql", json={'query': query_by_uri})
    assert response.status_code == 200, f"oops {response.text}"
    assert response.json().get('errors') is None, f"errors found {response.json()['errors']}"
    datasets = response.json()['data']['datasets']
    assert datasets[0]['uri'] == "images/test.tiff"


@pytest.mark.usefixtures("load_datasets")
def test_query_tags(rest_client: TestClient):
    response = rest_client.post("/graphql", json={'query': query_by_tags})
    assert response.status_code == 200, f"oops {response.text}"
    assert response.json().get('errors') is None, f"errors found {response.json()['errors']}"
    datasets = response.json()['data']['datasets']
    assert datasets[0]['uri'] == "images/test.tiff"


query_by_uri = """
query FindDatasets{
  datasets(uri: "images/test.tiff"){
    uri
    type
    tags {
      name
      accuracy
      locator
    }
  }
}"""

query_by_tags = """
query FindDatasets{
  datasets(tags: ["peaks"]){
    uri
    type
    tags {
      name
      accuracy
      locator
    }
  }
}"""