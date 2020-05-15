from tagging.tag_service import TagService
from pymongo import MongoClient
from databroker import catalog

def produce_training_data(num_images):
    db = MongoClient('mongo:')
    svc = TagService(db, db_name='ml_als')
    tag_sets = svc.find_tag_sets_one_filter('beamline', None)
    root_catalog = catalog('databroker')
    for tag_set in tag_sets:
        # write helper to find the value for tagset key
        asset_key = tag_set['asset_uid']
        catalog = root_catalog[asset_key]
        np_resource = catalog.thumbnails['np']
        array = np_resource()
