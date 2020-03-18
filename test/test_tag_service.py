import pytest
from pymongo.errors import DuplicateKeyError
import mongomock
from etl.tag_service import TagService


@pytest.fixture
def mongodb():
    return mongomock.MongoClient().db


@pytest.fixture
def tag_svc(mongodb):
    return TagService(mongodb)


def test_unique_uid_tag_set(tag_svc):
    tag_svc.create_tag_set(test_tags)
    with pytest.raises(DuplicateKeyError):
        tag_svc.create_tag_set({'uid': test_tags['uid']})


def test_random_sets(tag_svc):
    for x in range(10):
        tag_svc.create_tag_set({'uid': str(x)})
    cursor = tag_svc.get_random_tag_sets(3)
    count = 0
    for tag_set in cursor:
        count += 1
    assert count == 3


def test_get_tags(tag_svc):
    tag_svc.create_tag_set({'tags': [
                           {'frame_material': 'steel'},
                           {'color': 'purple'},
                           {'num_spokes': 36}]})

    tag_svc.create_tag_set({'tags': [
                           {'frame_material': 'steel'},
                           {'color': 'red'},
                           {'num_spokes': 36}]})

    tag_svc.create_tag_set({'tags': [
                           {'frame_material': 'carbon'},
                           {'num_spokes': 3}]})

    tag_svc.create_tag_set({'tags': [
                           {'color': 'red'},
                           {'num_spokes': 24}]})

    cursor = tag_svc.get_tag_sets_one_filter('color', 'red')
    assert count_results(cursor) == 2

    cursor = tag_svc.get_tag_sets_multi_filter([('color', 'red'), ('frame_material', 'steel')])
    assert count_results(cursor) == 1

    mongo_filter = {'tags.num_spokes': {'$gt': 3}}
    cursor = tag_svc.get_tag_sets_mongo(mongo_filter)
    assert count_results(cursor) == 3


def count_results(cursor):
    counter = 0
    for tag_set in cursor:
        counter += 1
    return counter


def test_tag_set(tag_svc):
    tag_svc.create_tag_set(test_tags)
    return_tags = tag_svc.get_tag_set(test_tags['uid'])
    return_tag_set = tag_svc.get_tag_set(return_tags['uid'])
    assert return_tag_set['uid'] == return_tags['uid']


test_tags = {
 "tags": [
   {
     "key": "scattering_geometry",
     "value": "transmission",
     "confidence": 0.9008, 
     "tag_event": "8c600210432b8f81ad229c1f",
   },
   {
     "key": "sample_detector_distance_name",
     "value": "WAXS",
     "confidence": 0.001, 
     "tag_event": "8c600210432b8f81ad229c1f",
   }
 ]
}
