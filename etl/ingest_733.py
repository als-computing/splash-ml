import etl.ingest
import datetime
import os
import logging
import pytest
import suitcase
import pymongo
import fnmatch
import glob
import uuid
from dotenv import load_dotenv
from tagging.tag_service import TagService
from suitcase.mongo_normalized import Serializer

logger = logging.getLogger('ingest_733')

# todo - change to layout that works with models from tensorflow.
def tagging_callback(path):
    metadata = dict()
    
    tag = get_tags('sample_detector_distance_name', path, ['saxs', 'waxs'])
    if tag is not None:
        metadata['sample_detector_distance_name'] = tag
        
    # get scattering_geometry
    tag = get_tags('scattering_geometry', path, ['gisaxs','giwaxs'])
    if tag is not None:
        metadata['scattering_geometry'] = tag

    # get beamline

    tag = get_tags('beamline', path, ['beamstop', 'auto'])
    if tag is not None:
        metadata['beamline'] = tag

    # get calibrant
    tag = get_tags('calibrant', path, ['agb', 'lab6'])
    if tag is not None:
        metadata['calibrant'] = tag

    return metadata


def run_meta_data_callback(path):
    metadata=dict()


    
    return metadata

# todo this only returns the first tag it finds
def get_tags(tag_name, path, kwrds):
    """takes as input the filepath and tuple of strings to search for in the path
    returns the string found. in None found, None is returned.
    
    parameters
    ----------
    path : [type]
        [description]
    kwrds : [type]
        [description]
    
    returns
    -------
    [type]
        [description]
    """
    for tag_value in kwrds:
        newStr = '*' + tag_value + '*'
        if fnmatch.fnmatch(path.lower(), newStr):
            return tag_value
    


def main():

    load_dotenv(verbose=True)
    input_root = os.getenv('input_root')
    output_root = os.getenv('output_root')
    msg_pack_dir = os.getenv('msg_pack_dir')
    paths = glob.glob(os.path.join(input_root,
            os.getenv('input_relative'))+'/**/*.*', recursive=True)
    logger.info(paths)
    etl_executor = etl.ingest.ETLExecutor(input_root, output_root, tagging_callback)

    db = pymongo.MongoClient(
            username=os.getenv('tag_db_user'),
            password=os.getenv('tag_db_password'),
            host=os.getenv('tag_db_host'))

    tag_svc = TagService(db, db_name='tagging')
    databroker_db_name = os.getenv('tag_databroker_db')
    serializer = Serializer(
        db[databroker_db_name],
        db[databroker_db_name])
    now = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc) \
            .astimezone().replace(microsecond = 0).isoformat()
    tagging_event = {
        "model_name": "scattering ingestor",
        "md":[
            {"key": "date", "value": now}
        ]
    }


    # Tagger, Event, and Asset are all hard coded in right now for testing
    # purposes.  This should not be kept in further versions
    tagger_uid = tag_svc.create_tagger({'uid':None, 'type':'model',
            'model_name':'hard_code', 'create_time':10, 'accuracy': 0.01})
    tagging_event_uid = tag_svc.create_tagging_event({'uid':None,
            'tagger_id':tagger_uid, 'run_time':20},tagger_uid)
    
    count = 1
    for file_path in paths:
        print(file_path)
        if os.path.getsize(file_path) = 0  :
            continue


        try:
            raw_metadata, thumb_metadatas, return_metadata = etl_executor.execute(
                    file_path, [(223, 'jpg'), (223, 'npy')])
        except TypeError as e:
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(e)
            raise e
        else:          
            docs = etl.ingest.createDocument(raw_metadata, output_root,
                    thumb_metadatas, return_metadata)
            
            for name, doc in docs:
                serializer(name, doc)
                if name == 'start':
                    tag_set = make_tag_set(doc, return_metadata)
                    tag_svc.create_asset_tags(tag_set, tagging_event_uid)
                    count += 1


def make_tag_set(start_doc, tags_dict):
    if tags_dict is None:
        return None
    run_uid = start_doc['uid']
    tags = []
    tags.extend([
            {
                "tag": "rods",
                "confidence": 0.9008,
                "event_id": None
            },
            {
                "tag": "peaks",
                "confidence": 0.001, 
                "event_id": None
            }])
    asset_tags = {
        'uid': run_uid,
        'sample_id': 'house_paint_1234',
        'tags': tags
    }
    return asset_tags

if __name__ == '__main__':
    main()
