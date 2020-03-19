import pytest
from pymongo.errors import DuplicateKeyError
import mongomock
from tagging.tag_service import TagService


@pytest.fixture
def mongodb():
    return mongomock.MongoClient().db


@pytest.fixture
def tag_svc(mongodb):
    return TagService(mongodb)


def test_unique_uid_tag_set(tag_svc):
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event)
    tag_svc.create_tag_set(test_tags, tagging_event_uid)
    with pytest.raises(DuplicateKeyError):
        tag_svc.create_tag_set({'uid': test_tags['uid'], 'tags': [{'key': 'foo', 'value': 'bar'}]}, tagging_event_uid)

def test_random_sets(tag_svc):
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event)
    for x in range(10):
        tag_svc.create_tag_set({'tags': [{'key': 'foo', 'value': 'bar'}]}, tagging_event_uid)
    cursor = tag_svc.find_random_tag_sets(3)
    assert count_results(cursor) == 3


def test_get_tags(tag_svc):
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event)
    tag_svc.create_tag_set({'tags': [
                           {'frame_material': 'steel'},
                           {'color': 'purple'},
                           {'num_spokes': 36}]}, tagging_event_uid)

    tag_svc.create_tag_set({'tags': [
                           {'frame_material': 'steel'},
                           {'color': 'red'},
                           {'num_spokes': 36}]}, tagging_event_uid)

    tag_svc.create_tag_set({'tags': [
                           {'frame_material': 'carbon'},
                           {'num_spokes': 3}]}, tagging_event_uid)

    tag_svc.create_tag_set({'tags': [
                           {'color': 'red'},
                           {'num_spokes': 24}]}, tagging_event_uid)

    cursor = tag_svc.find_tag_sets_one_filter('color', 'red')
    assert count_results(cursor) == 2

    cursor = tag_svc.find_tag_sets_multi_filter([('color', 'red'), ('frame_material', 'steel')])
    assert count_results(cursor) == 1

    mongo_filter = {'tags.num_spokes': {'$gt': 3}}
    cursor = tag_svc.find_tag_sets_mongo(mongo_filter)
    assert count_results(cursor) == 3


def count_results(cursor):
    counter = 0
    for tag_set in cursor:
        counter += 1
    return counter


def test_tag_event(tag_svc):
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event)
    assert tagging_event_uid is not None
    return_tagging_event = tag_svc.get_tagging_event(tagging_event_uid)
    assert return_tagging_event.get('schema_version') == TagService.SCHEMA_VERSION


def test_tag_set(tag_svc):
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event)
    tag_svc.create_tag_set(test_tags, tagging_event_uid)
    return_tag_set = tag_svc.find_tag_set(test_tags['uid'])
    assert return_tag_set.get('schema_version') == TagService.SCHEMA_VERSION
    
    for tag in return_tag_set['tags']:
        assert tag['tag_event'] == tagging_event_uid

    return_tag_set = tag_svc.find_tag_set(return_tag_set['uid'])
    assert return_tag_set['uid'] == return_tag_set['uid']


test_tags = {
 "asset_uid": "ee600210432b8f81ad229c33",
 "tags": [
   {
     "key": "scattering_geometry",
     "value": "transmission",
     "confidence": 0.9008,
   },
   {
     "key": "sample_detector_distance_name",
     "value": "WAXS",
     "confidence": 0.001, 
   }
 ],
}

tagging_event = {
    "model_name": "PyTestNet"
}
