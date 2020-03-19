import etl.ingest
import datetime
import os
import logging
import pytest
import suitcase
import pymongo
import fnmatch
import glob
from dotenv import load_dotenv
from athena.tag_service import TagService
from suitcase.mongo_normalized import Serializer

logger = logging.getLogger('ingest_733')


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

# TODO this only returns the first tag it finds
def get_tags(tag_name, path, kwrds):
    """Takes as input the filepath and tuple of strings to search for in the path
    Returns the string found. In none found, None is returned.
    
    Parameters
    ----------
    path : [type]
        [description]
    kwrds : [type]
        [description]
    
    Returns
    -------
    [type]
        [description]
    """
    # tags = []
    for tag_value in kwrds:
        newStr = '*' + tag_value + '*'
        if fnmatch.fnmatch(path.lower(), newStr):
            # strOut.append(tag_value)
            # tags.append(tag)
            return tag_value
    # return tags


def main():

    load_dotenv(verbose=True)
    input_root = os.getenv('input_root')
    output_root = os.getenv('output_root')
    msg_pack_dir = os.getenv('msg_pack_dir')
    paths = glob.glob(os.path.join(input_root, os.getenv('input_relative')))
    logger.info(paths)
    etl_executor = etl.ingest.ETLExecutor(input_root, output_root, tagging_callback)

    db = pymongo.MongoClient(
            username=os.getenv('tag_db_user'),
            password=os.getenv('tag_db_password'),
            host=os.getenv('tag_db_host'),
            authSource='admin')

    tag_svc = TagService(db, db_name='ml_als')

    serializer = Serializer(
        db.ml_als,
        db.ml_als)
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone().replace(microsecond=0).isoformat()
    tagging_event = {
        "model_name": "scattering ingestor",
        "md":[
            {"key": "date", "value": now}
        ]
    }

    tag_event_uid = tag_svc.create_tagging_event(tagging_event)

    for file_path in paths:
        try:
            raw_metadata, thumb_metadatas, return_tags = etl_executor.execute(
                                                file_path, [(256, 'jpg'), (256, 'tiff')])
        except TypeError as e:
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(e)
            raise e
        else:          
            docs = etl.ingest.createDocument(raw_metadata, output_root, thumb_metadatas)
            for name, doc in docs:
                serializer(name, doc)
                if name == 'start':
                    tag_set = make_tag_set(doc, return_tags)
                    tag_svc.create_tag_set(tag_set, tag_event_uid)


def make_tag_set(start_doc, tags_dict):
    if tags_dict is None:
        return None
    run_uid = start_doc['uid']
    tags = []
    for key in tags_dict.keys():
        tags.append({'key': key, 'value': tags_dict[key]})

    tag_set = {
        'asset_uid': run_uid,
        'tags': tags
    }
    return tag_set

if __name__ == '__main__':
    main()
