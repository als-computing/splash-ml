import etl.ingest
import os
import logging
from mongoengine import connect
import pytest
import suitcase
import pymongo
import fnmatch

logger = logging.getLogger('ingest_733')

def ingest_733(raw_metadata, output_root, thumbnail_medadatas=None):
    yield etl.ingest.createDocument(raw_metadata, output_root, thumbnail_medadatas)


def tagging_callback(path):
    scattering_geometry = None
    sample_detector_distance_name = None
    metadata = dict()
    
    # get sample_detector_distance_name
    sample_detector_distance_name = ['saxs','waxs']
    sample_detector_distance_name = get_tags(path, sample_detector_distance_name)
    if sample_detector_distance_name:
        metadata['sample_detector_distance_name'] = sample_detector_distance_name
        
    # get scattering_geometry
    scattering_geometry = ['gisaxs','giwaxs']
    scattering_geometry = get_tags(path, scattering_geometry)
    if scattering_geometry:
        metadata['scattering_geometry'] = scattering_geometry

    # get beamline
    beamline = ['beamstop','auto']
    beamline = get_tags(path, beamline)
    if beamline:
        metadata['beamline'] = beamline

    # get calibrant
    calibrant = ['agb','lab6']
    calibrant = get_tags(path, calibrant)
    if calibrant:
        metadata['calibrant'] = calibrant   
    
    return metadata  

def get_tags(path, kwrds):
    ## Takes as input the filepath and tuple of strings to search for in the path
    ## Returns the string found. In none found, None is returned.
    strOut = []
    for x in kwrds:
        newStr = '*' + x + '*'
        if fnmatch.fnmatch(path.lower(), newStr):
            strOut.append(x)
    if strOut:
        return strOut
    else:
        return None
    
def main():
    import glob
    from dotenv import load_dotenv
    from etl.tag_service import TagService
    load_dotenv(verbose=True)
    # from suitcase.msgpack import export
    # from suitcase.jsonl import export
    from suitcase.mongo_normalized import Serializer
    input_root = os.getenv('input_root')
    output_root = os.getenv('output_root')
    msg_pack_dir = os.getenv('msg_pack_dir')
    paths = glob.glob(os.path.join(input_root, os.getenv('input_relative')))
    logger.info(paths)
    etl_executor = etl.ingest.ETLExecutor(input_root, output_root, tagging_callback)

    connect(db=os.getenv('tag_db_name'),
            username=os.getenv('tag_db_user'),
            password=os.getenv('tag_db_password'),
            host=os.getenv('tag_db_host'))

    db = pymongo.MongoClient(
            username=os.getenv('tag_db_user'),
            password=os.getenv('tag_db_password'),
            host=os.getenv('tag_db_host'),
            authSource='admin')

    # tag_svc = TagService()
    serializer = Serializer( 
        db.ml_als, 
        db.ml_als)
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
            # docs = ingest_733(raw_metdata, output_root, thumb_metadatas            
            docs = etl.ingest.createDocument(raw_metadata, output_root, thumb_metadatas)
            for name, doc in docs:
                serializer(name, doc)

            # relative_path = os.path.relpath(raw_metdata.path, output_root)
            # tag_svc.create_image(raw_metdata.hash, relative_path, return_tags)
            # export(docs, msg_pack_dir)
            # with Serializer(directory, file_prefix, cls=cls, **kwargs) as serializer:
            #     for item in gen:
            #         serializer(*item)
  
if __name__ == '__main__':
    main()
