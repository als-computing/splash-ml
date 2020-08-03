import pytest
import pymongo
from pymongo.errors import DuplicateKeyError
import mongomock
from tagging.tag_service import TagService
from tagging.schemas import schema_asset_tags, schema_tagging_event, schema_tagger
from .validation_test_data import asset_tags_good, tagging_events_good, taggers_good
from .validation_test_data import asset_tags_bad, tagging_events_bad, taggers_bad
from .validation_test_data import json_datetime
import jsonschema


@pytest.fixture
def mongodb():
    return mongomock.MongoClient().db
    # return pymongo.MongoClient()


@pytest.fixture
def tag_svc(mongodb):
    return TagService(mongodb)


# test validate (returns errors list)
# should be no errors if good data, some errors if bad data
def validator_test(tag_svc, data_set, schema, data_good=True):
    for data in data_set:
        return_errors = tag_svc.validate_json(data, schema)
        if data_good:
            assert len(return_errors) == 0
        else:
            assert len(return_errors) > 0


def test_validator_good(tag_svc):
    validator_test(tag_svc, asset_tags_good, schema_asset_tags)
    validator_test(tag_svc, tagging_events_good, schema_tagging_event)
    validator_test(tag_svc, taggers_good, schema_tagger)


def test_validator_bad(tag_svc):
    # TODO: perhaps go more detailed into exactly
    #       how many errors each bad datum should have
    validator_test(tag_svc, asset_tags_bad, schema_asset_tags, False)
    validator_test(tag_svc, tagging_events_bad, schema_tagging_event, False)
    validator_test(tag_svc, taggers_bad, schema_tagger, False)


def test_unique_uid_tag_set(tag_svc):
    tagger_uid = tag_svc.create_tagger(tagger)
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event, tagger_uid)
    asset_tags_uid = tag_svc.create_asset_tags(asset_tags, tagging_event_uid)
    with pytest.raises(DuplicateKeyError):
        tag_svc.create_tagger({'uid': tagger['uid'], "type": "model",
                "model_name": "netty", "create_time": json_datetime(2)})
    with pytest.raises(DuplicateKeyError):
        tag_svc.create_tagging_event({'uid': tagging_event['uid'], "tagger_id":
            tagger['uid'],"run_time": json_datetime(3), "accuracy": 1}, tagger_uid)
    with pytest.raises(DuplicateKeyError):
        tag_svc.create_asset_tags({'uid': asset_tags['uid'], "sample_id": "plants", "tags": []},
            tagging_event_uid)


def test_random_sets(tag_svc):
    tagger_uid = tag_svc.create_tagger(tagger)
    for x in range(10):
        tagging_event_uid = tag_svc.create_tagging_event({'uid': None, "tagger_id":
                None, "run_time": json_datetime(3), "accuracy": 0.5}, tagger_uid)
        asset_tags_uid = tag_svc.create_asset_tags({'uid': None, "sample_id":
                "plants", "tags": []}, tagging_event_uid)
    cursor = tag_svc.find_random_event_sets(3)
    assert count_results(cursor) == 3
    cursor = tag_svc.find_random_asset_sets(3)
    assert count_results(cursor) == 3


def test_get_events_and_tags(tag_svc):
    tagger_uid = tag_svc.create_tagger(tagger)

    tagging_event_uid = tag_svc.create_tagging_event({
            'uid': None,
            'tagger_id': None,
            'run_time': json_datetime(1134433.223),
            'accuracy': 0.5678}, tagger_uid)
    asset_tags_uid = tag_svc.create_asset_tags({
            'uid': None,
            'sample_id': 'house paint 1234',
            'tags': [{
                    'tag': 'rods',
                    'confidence': 0.908,
                    'event_id': None,
                },{
                    'tag': 'giwaxs',
                    'confidence': 0.092, 
                    'event_id': None,
                }]}, tagging_event_uid)


    tagging_event_uid = tag_svc.create_tagging_event({
            'uid': None,
            'tagger_id': None,
            'run_time': json_datetime(0),
            'accuracy': 0}, tagger_uid)
    asset_tags_uid = tag_svc.create_asset_tags({
            'uid': None,
            'sample_id': 'paint',
            'tags': [{
                    'tag': 'beamline',
                    'confidence': 0.998,
                    'event_id': None,
                },{
                    'tag': 'giwaxs',
                    'confidence': 0.002, 
                    'event_id': None,
                }]}, tagging_event_uid)

     
    tagging_event_uid = tag_svc.create_tagging_event({
            'uid': None,
            'tagger_id': None,
            'run_time': json_datetime(23),
            'accuracy': 0.1234}, tagger_uid)
    asset_tags_uid = tag_svc.create_asset_tags({
            'uid': None,
            'sample_id': 'house',
            'tags': [{
                    'tag': 'rings',
                    'confidence': 0.998,
                    'event_id': None,
                },{
                    'tag': 'giwaxs',
                    'confidence': 0.002, 
                    'event_id': None,
                }]}, tagging_event_uid)


    cursor = tag_svc.find_tags_one_filter('giwaxs', 0.002)
    assert count_results(cursor) == 2

    mongo_filter = {'tags.tag': 'giwaxs', 'tags.confidence': {'$gt': 0}}
    cursor = tag_svc.find_asset_sets_mongo(mongo_filter)
    assert count_results(cursor) == 3

    # Commented out - Using ISO 8601 over Unix time
    # mongo_filter = {'run_time': {'$gt': 0}}
    # cursor = tag_svc.find_events_mongo(mongo_filter)
    # assert count_results(cursor) == 2

    mongo_filter = {'accuracy': {'$gt': 0}}
    cursor = tag_svc.find_events_mongo(mongo_filter)
    assert count_results(cursor) == 2
    
    mongo_filter = {'type': 'model'}
    cursor = tag_svc.find_tagger_mongo(mongo_filter)
    assert count_results(cursor) == 1


def count_results(cursor):
    counter = 0
    for data_set in cursor:
        counter += 1
    return counter


def test_tagger(tag_svc):
    tagger_uid = tag_svc.create_tagger(tagger)
    assert tagger_uid is not None
    return_tagger = tag_svc.get_tagger(tagger_uid)
    assert return_tagger.get('schema_version') == TagService.SCHEMA_VERSION


def test_tag_set(tag_svc):
    tagger_uid = tag_svc.create_tagger(tagger)
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event, tagger_uid)
    tag_svc.create_asset_tags(asset_tags, tagging_event_uid)
    return_asset_set = tag_svc.find_asset_set(asset_tags['uid'])
    assert return_asset_set.get('schema_version') == TagService.SCHEMA_VERSION
   
    for tags in asset_tags['tags']:
        assert tags['event_id'] == tagging_event_uid

    return_asset_set2 = tag_svc.find_asset_set(return_asset_set['uid'])
    assert return_asset_set2['uid'] == return_asset_set['uid']


def test_add_asset_tags(tag_svc):
    tagger_uid = tag_svc.create_tagger(tagger)
    tagging_event_uid = tag_svc.create_tagging_event(tagging_event, tagger_uid)
    asset_tags_uid = tag_svc.create_asset_tags(asset_tags, tagging_event_uid)
    
    return_asset_set = tag_svc.add_asset_tags([{
            "tag": "add1",
            "confidence": 0.50,
            "event_id": "wwewere6002104rwerwe81ad229c33",
        },{
            "tag": "add2",
            "confidence": 0.50, 
            "event_id": "wwewere6002104rwerwe81ad229c33",
        }], asset_tags_uid, tagging_event_uid)

    assert len(return_asset_set['tags']) == 4


# this was test_tags before
asset_tags = {
    "uid": None,
    "sample_id": "house paint 1234",
    "tags": [
        {
            "tag": "rods",
            "confidence": 0.9008,
            "event_id": None,
        },
        {
            "tag": "peaks",
            "confidence": 0.001, 
            "event_id": None,
        }
    ]
}
# name didnt change, though it doesnt carry model_name anymore
tagging_event = {
    "uid": None,
    "tagger_id": None,
    "run_time": json_datetime(1134433.223),
    "accuracy": 0.7776
}
# takes place of tagging_event as the parent dir
tagger = {
    "uid": None,
    "type": "model",
    "model_name": "PyTestNet",
    "create_time": json_datetime(11112333.3)
}
