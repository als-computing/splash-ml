import datetime
import pytest

from pymongo.errors import DuplicateKeyError
import mongomock
from ..tag_service import TagService
from ..model import (
    SCHEMA_VERSION,
    Dataset,
    DatasetType,
    Tag,
    TagSource,
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
    tagger = tag_svc.create_tag_source(new_tagger)
    new_tagging_event.tagger_id = tagger.uid
    tagging_event = tag_svc.create_tagging_event(new_tagging_event)
    asset = tag_svc.create_dataset(new_asset)
    with pytest.raises(DuplicateKeyError):
        another_tagger = TagSource(**
                {"uid": tagger.uid,
                 "type": "model",
                 "name": "netty"})
        tag_svc.create_tag_source(another_tagger)
           
    with pytest.raises(DuplicateKeyError):
        another_event = TaggingEvent(**
            {"uid": tagging_event.uid,
             "tagger_id": tagger.uid,
             "run_time": json_datetime(3),
             "accuracy": 1})
        tag_svc.create_tagging_event(another_event)

    with pytest.raises(DuplicateKeyError):
        another_asset = Dataset(**{
            "uid": asset.uid,
            "type": "file",
            "uri": "bar"})
        tag_svc.create_dataset(another_asset)


def test_create_and_find_tagger(tag_svc: TagService):
    tagger = tag_svc.create_tag_source(new_tagger)
    assert tagger is not None
    return_taggers = tag_svc.find_tag_sources(name="PyTestNet")
    assert list(return_taggers)[0].name == "PyTestNet"


def test_create_and_find_tagging_event(tag_svc: TagService):
    tagger = tag_svc.create_tag_source(new_tagger)
    tagging_event = tag_svc.create_tagging_event(TaggingEvent(tagger_id=tagger.uid, run_time=datetime.datetime.now()))

    return_tagging_event = tag_svc.retrieve_tagging_event(tagging_event.uid)
    assert tagging_event.uid == return_tagging_event.uid
    assert tagging_event.tagger_id == return_tagging_event.tagger_id


def test_create_and_find_asset(tag_svc: TagService):
    tagger = tag_svc.create_tag_source(new_tagger)
    new_tagging_event.tagger_id = tagger.uid
    asset = tag_svc.create_dataset(new_asset)
    return_asset = tag_svc.retrieve_dataset(asset.uid)

    assert return_asset.schema_version == SCHEMA_VERSION

    for tag in asset.tags:
        if tag.name == "geometry":
            assert tag.event_id == "import_id1"

    returns_asset_from_search = list(tag_svc.find_datasets(tags=["rods"]))
    assert len(returns_asset_from_search) == 1
    assert return_asset == returns_asset_from_search[0], "Search and retrieve return same"


def test_add_asset_tags(tag_svc: TagService):
    asset = tag_svc.create_dataset(new_asset)
    tagging_event = tag_svc.create_tagging_event(new_tagging_event)
    new_tag = Tag(**{
            "name": "add1",
            "confidence": 0.50,
            "event_id": tagging_event.uid,
    })
    return_asset_set = tag_svc.add_tags([new_tag], asset.uid)

    assert len(return_asset_set.tags) == 4


def test_add_none_tags(tag_svc: TagService):
    asset = tag_svc.create_dataset(no_tag_asset)
    new_tag = no_tag
    return_asset_set = tag_svc.add_tags([new_tag], asset.uid)

    assert return_asset_set.tags[0].name == 'rod'


new_asset = Dataset(**{
    "sample_id": "house paint 1234",
    "type": DatasetType.file,
    "uri": "images/test.tiff",
    "tags": [
        {
            "name": "rods",
            "confidence": 0.9008,
            "event_id": None,
        },
        {
            "name": "peaks",
            "confidence": 0.001,

        },
        {
            "name": "reflection",
            "confidence": 1,
            "event_id": "import_id1",
        }
    ]
})


# name didnt change, though it doesnt carry model_name anymore
new_tagging_event = TaggingEvent(**{
    "tagger_id": "Tricia McMillin",
    "run_time": json_datetime(1134433.223),
    "accuracy": 0.7776
})


new_tagger = TagSource(**{
    "type": "model",
    "name": "PyTestNet",
    "model_info": {
        "label_index": {
            "blue": 1,
            "red": 2
        }
    }
})

no_tag_asset = Dataset(**{
    "type": "file",
    "uri": "blahblahblah"
})

no_tag = Tag(**{
    "name": "rod",
    "tags": None
})
