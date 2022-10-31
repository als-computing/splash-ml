from fastapi.testclient import TestClient

from ..api import API_URL_PREFIX

from ..model import (
    Dataset,
    SearchDatasetsRequest,
    Tag,
    TagSource,
    TagPatchRequest
)


def test_taggers(rest_client: TestClient):
    response = rest_client.post(API_URL_PREFIX + "/tagsources", json=tag_source_1_dict)
    response = rest_client.post(API_URL_PREFIX + "/tagsources", json=tag_source_2_dict)
    response: TagSource = rest_client.get(API_URL_PREFIX + "/tagsources")
    tag_sources = response.json()
    assert len(tag_sources) == 2, "made two tag sources"


def test_tags_and_datasets(rest_client: TestClient):
    # create a dataset
    response = rest_client.post(API_URL_PREFIX + "/datasets", json=dataset)
    assert response.status_code == 200

    # search on name
    response: Dataset = rest_client.get(
        API_URL_PREFIX + "/datasets",
        params={"page[offset]": 0, "page[limit]": 10, "uri": "/foo/bar.h5"})
    assert response.status_code == 200, f"oops {response.text}"
    tag_sources = response.json()
    assert len(tag_sources) == 1

    # search on name using the post method
    response: Dataset = rest_client.post(
        API_URL_PREFIX + "/datasets/search",
        params={"page[offset]": 0, "page[limit]": 10},
        json={'uris': ['/foo/bar.h5']})
    assert response.status_code == 200, f"oops {response.text}"
    tag_sources = response.json()
    assert len(tag_sources) == 1

    # add a new tag
    new_tag = Tag(name="peaks", confidence=0.1)
    req = TagPatchRequest(add_tags=[new_tag])
    response = rest_client.patch(
        f"{API_URL_PREFIX}/datasets/{tag_sources[0]['uid']}/tags",
        json=req.dict())
    assert response.status_code == 200, f"oops {response.text}"

    # find the dataset based on tags
    response: Dataset = rest_client.get(
        API_URL_PREFIX + "/datasets",
        params={"skip": 0, "limit": 10, "tag_value": "peaks"})
    assert response.status_code == 200, f"oops {response.text}"
    tag_sources = response.json()
    assert len(tag_sources) == 1

    # delete first tag
    tag_uid = [tag_sources[0]['uid']]
    req = TagPatchRequest(remove_tags=tag_uid)
    response = rest_client.patch(
        f"{API_URL_PREFIX}/datasets/{tag_sources[0]['uid']}/tags",
        json=req.dict())
    assert response.status_code == 200, f"oops {response.text}"


def test_skip_limit(rest_client: TestClient):
    response = rest_client.post(API_URL_PREFIX + "/datasets", json=dataset)
    assert response.status_code == 200

    response = rest_client.post(API_URL_PREFIX + "/datasets", json=dataset2)
    assert response.status_code == 200

    response: Dataset = rest_client.get(
        API_URL_PREFIX + "/datasets",
        params={"page[offset]": 1, "page[limit]": 1})
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
