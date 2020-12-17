import datetime
import pytest
import pymongo
from pymongo.errors import DuplicateKeyError
import mongomock
from ..tag_service import TagService
from ..model import (
    LABEL_NAME,
    SCHEMA_VERSION,
    Asset,
    NewAsset,
    NewTagger,
    NewTaggingEvent,
    Tag,
    Tagger,
    TaggingEvent
)


def json_datetime(unix_timestamp):
    return datetime.datetime.utcfromtimestamp(unix_timestamp).isoformat() + "Z"


@pytest.fixture
def mongodb():
    return mongomock.MongoClient().db
    # return pymongo.MongoClient()


@pytest.fixture
def tag_svc(mongodb):
    return TagService(mongodb)


def test_unique_uid_tag_set(tag_svc: TagService):
    tagger = tag_svc.create_tagger(new_tagger)
    new_tagging_event.tagger_id = tagger.uid
    tagging_event = tag_svc.create_tagging_event(new_tagging_event)
    asset = tag_svc.create_asset(new_asset)
    with pytest.raises(DuplicateKeyError):
        another_tagger = NewTagger(**
                {"uid": tagger.uid,
                 "type": "model",
                 "name": "netty"})
        tag_svc.create_tagger(another_tagger)
           
    with pytest.raises(DuplicateKeyError):
        tag_svc.create_tagging_event(
            {"uid": tagging_event.uid,
             "tagger_id": tagger.uid,
             "run_time": json_datetime(3),
             "accuracy": 1},
            tagger.uid)
    with pytest.raises(DuplicateKeyError):
        tag_svc.create_asset(
            {"uid": asset.uid,
             "sample_id": "plants",
             "tags": []},
            tagging_event.uid)


def test_create_and_find_tagger(tag_svc: TagService):
    tagger = tag_svc.create_tagger(new_tagger)
    assert tagger is not None
    return_taggers = tag_svc.find_taggers(name="PyTestNet")
    assert list(return_taggers)[0].name == "PyTestNet"


def test_create_and_find(tag_svc: TagService):
    tagger = tag_svc.create_tagger(new_tagger)
    new_tagging_event.tagger_id = tagger.uid
    asset = tag_svc.create_asset(new_asset)
    return_asset = tag_svc.retrieve_asset(asset.uid)

    assert return_asset.schema_version == SCHEMA_VERSION

    for tag in asset.tags:
        if tag.name == "geometry":
            assert tag.event_id == "import_id1"

    returns_asset_from_search = list(tag_svc.find_assets(**{"tags.value": "rods"}))[0]
    assert return_asset == returns_asset_from_search, "Search and retrieve return same"


def test_add_asset_tags(tag_svc: TagService):
    asset = tag_svc.create_asset(new_asset)
    tagging_event = tag_svc.create_tagging_event(new_tagging_event)
    new_tag = Tag(**{
            "name": LABEL_NAME,
            "value": "add1",
            "confidence": 0.50,
            "event_id": tagging_event.uid,
    })
    return_asset_set = tag_svc.add_tags([new_tag], asset.uid)

    assert len(return_asset_set.tags) == 4


new_asset = NewAsset(**{
    "sample_id": "house paint 1234",
    "asset_locator": {
        "uid": "42",
        "type": "file",
        "locator": "images/test.tiff"
    },
    "tags": [
        {
            "name": LABEL_NAME,
            "value": "rods",
            "confidence": 0.9008,
            "event_id": None,
        },
        {
            "name": LABEL_NAME,
            "value": "peaks",
            "confidence": 0.001,

        },
        {
            "name": "geometry",
            "value": "reflection",
            "confidence": 1,
            "event_id": "import_id1",
        }
    ]
})


# name didnt change, though it doesnt carry model_name anymore
new_tagging_event = NewTaggingEvent(**{
    "tagger_id": "Tricia McMillin",
    "run_time": json_datetime(1134433.223),
    "accuracy": 0.7776
})


new_tagger = NewTagger(**{
    "type": "model",
    "name": "PyTestNet"
})
