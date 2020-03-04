import ingest
import os
import logging
from mongoengine import connect
logger = logging.getLogger('ingest_733')

def ingest_733(raw_metadata, output_root, thumbnail_medadatas=None):
    yield from ingest.createDocument(raw_metadata, output_root, thumbnail_medadatas)

def tagging_callback(path):
    import fnmatch
    scattering_geometry = None
    sample_detector_distance_name = None
    metadata = dict()
    #get sample_detector_distance
    if fnmatch.fnmatch(path.lower(), '*saxs*'):
        sample_detector_distance_name = 'SAXS'
    elif fnmatch.fnmatch(path.lower(), '*waxs*'):
        sample_detector_distance_name = 'WAXS'

    if sample_detector_distance_name:
        #get scattering_geometry
        if fnmatch.fnmatch(path.lower(), '*gisaxs*') or fnmatch.fnmatch(path.lower(), '*giwaxs*') :
            scattering_geometry = 'reflection' 
        else:
            scattering_geometry = 'transmission'
        metadata['scattering_geometry'] = scattering_geometry
        metadata['sample_detector_distance_name'] = sample_detector_distance_name
    return metadata

def main():
    import glob
    from dotenv import load_dotenv
    from tag_service import TagService
    load_dotenv(verbose=True)
    # from suitcase.msgpack import export
    from suitcase.jsonl import export
    from suitcase.mongo_embedded import Serializer
    input_root = os.getenv('input_root')
    output_root = os.getenv('output_root')
    msg_pack_dir = os.getenv('msg_pack_dir')
    paths = glob.glob(os.path.join(input_root, os.getenv('input_relative')))
    logger.info(paths)
    etl_executor = ingest.ETLExecutor(input_root, output_root, tagging_callback)

    connect(db=os.getenv('tag_db_name'),
            username=os.getenv('tag_db_user'),
            password=os.getenv('tag_db_password'),
            host=os.getenv('tag_db_host'))
    tag_svc = TagService()
    for file_path in paths:
        try:
            raw_metdata, thumb_metadatas, return_tags = etl_executor.execute(
                                                file_path, [(256, 'jpg'), (256, 'tiff')])
        except TypeError as e:
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(e)
            raise e
        else:
            docs = ingest_733(raw_metdata, output_root, thumb_metadatas)
            relative_path = os.path.relpath(raw_metdata.path, output_root)
            tag_svc.create_image(raw_metdata.hash, relative_path, return_tags)
            export(docs, msg_pack_dir)
            # with Serializer(directory, file_prefix, cls=cls, **kwargs) as serializer:
            #     for item in gen:
            #         serializer(*item)
  
if __name__ == '__main__':
    main()