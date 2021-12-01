import datetime
import pytest

from pymongo.errors import DuplicateKeyError

from ..tag_service import TagService
from ..model import (
    SCHEMA_VERSION,
    Dataset,
    Tag,
    TagPatchRequest,
    TagSource,
    TaggingEvent
)

from .data import (
    json_datetime,
    new_dataset,
    new_tagging_event,
    new_tagger,
    no_tag_dataset,
    no_tag
)


def test_unique_uid_tag_set(tag_svc: TagService):
    tagger = tag_svc.create_tag_source(new_tagger)
    new_tagging_event.tagger_id = tagger.uid
    tagging_event = tag_svc.create_tagging_event(new_tagging_event)
    asset = tag_svc.create_dataset(new_dataset)
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
        another_dataset = Dataset(**{
            "uid": asset.uid,
            "type": "file",
            "uri": "bar"})
        updated_dataset = tag_svc.create_dataset(another_dataset)
        # create dataset will not raise the error by itself because it overwrites the UID
        # To force duplicate:
        tag_svc._collection_dataset.update_one({'uid': updated_dataset.uid},
                                               {'$set': {'uid': asset.uid}}
                                               )


def test_create_and_find_tagger(tag_svc: TagService):
    tagger = tag_svc.create_tag_source(new_tagger)
    assert tagger is not None
    return_taggers = tag_svc.find_tag_sources(name="PyTestNet")
    assert list(return_taggers)[0].name == "PyTestNet"


def test_create_and_find_tagging_event(tag_svc: TagService):
    tagger = tag_svc.create_tag_source(new_tagger)
    tagging_event = tag_svc.create_tagging_event(TaggingEvent(
                                                    tagger_id=tagger.uid,
                                                    run_time=datetime.datetime.now()))

    return_tagging_event = tag_svc.retrieve_tagging_event(tagging_event.uid)
    assert tagging_event.uid == return_tagging_event.uid
    assert tagging_event.tagger_id == return_tagging_event.tagger_id


def test_create_and_find_asset(tag_svc: TagService):
    tagger = tag_svc.create_tag_source(new_tagger)
    new_tagging_event.tagger_id = tagger.uid
    asset = tag_svc.create_dataset(new_dataset)
    return_asset = tag_svc.retrieve_dataset(asset.uid)

    assert return_asset.schema_version == SCHEMA_VERSION

    for tag in asset.tags:
        if tag.name == "geometry":
            assert tag.event_id == "import_id1"

    returns_asset_from_search = list(tag_svc.find_datasets(tags=["rods"]))
    assert len(returns_asset_from_search) > 0
    found_asset = False
    for return_asset in returns_asset_from_search:
        if return_asset == returns_asset_from_search[0]:
            found_asset = True
            break
    assert found_asset, "Search and retrieve return same"


def test_add_dataset_tags(tag_svc: TagService):
    dataset = tag_svc.create_dataset(new_dataset)
    tagging_event = tag_svc.create_tagging_event(new_tagging_event)
    new_tag = Tag(**{
            "name": "add1",
            "confidence": 0.50,
            "event_id": tagging_event.uid,
            "locator": {
                "spec": "test_locator",
                "path": ["foo", "bar"]
            }
    })
    req = TagPatchRequest(add_tags=[new_tag], remove_tags=[])
    added_tags_uids = tag_svc.modify_tags(req, dataset.uid)
    updated_dataset = tag_svc._collection_dataset.find_one({'tags.uid': added_tags_uids[0][0]})
    tag_svc._clean_mongo_ids(updated_dataset)
    updated_dataset = Dataset.parse_obj(updated_dataset)
    assert len(updated_dataset.tags) == 4


def test_add_none_tags(tag_svc: TagService):
    dataset = tag_svc.create_dataset(no_tag_dataset)
    req = TagPatchRequest(add_tags=[no_tag], remove_tags=[])
    added_tags_uids = tag_svc.modify_tags(req, dataset.uid)
    updated_dataset = tag_svc._collection_dataset.find_one({'tags.uid': added_tags_uids[0][0]})
    tag_svc._clean_mongo_ids(updated_dataset)
    updated_dataset = Dataset.parse_obj(updated_dataset)
    assert updated_dataset.tags[0].name == 'rod'


def test_remove_dataset_tags(tag_svc: TagService):
    # this test creates a new dataset with 3 tags, and deletes the first 2
    dataset = tag_svc.create_dataset(new_dataset)
    remove_tags_uids = [dataset.tags[0].uid, dataset.tags[1].uid]
    req = TagPatchRequest(add_tags=[], remove_tags=remove_tags_uids)
    output = tag_svc.modify_tags(req, dataset.uid)
    updated_dataset = tag_svc.retrieve_dataset(dataset.uid)
    assert len(updated_dataset.tags) == 1 and (remove_tags_uids == output[1])


def test_remove_nonexistent_tag(tag_svc: TagService):
    # this test creates a dataset with no tags and deletes a nonexistent tag
    dataset = tag_svc.create_dataset(no_tag_dataset)
    remove_tags_uids = ["123"]
    req = TagPatchRequest(add_tags=[], remove_tags=remove_tags_uids)
    deleted_tags_uids = tag_svc.modify_tags(req, dataset.uid)
    assert deleted_tags_uids[1][0] == '-1'
