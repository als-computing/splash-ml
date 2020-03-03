import os
import time
import fabio
import event_model
import logging
import numpy as np
from PIL import Image, ImageOps
from skimage.transform import resize
import imageio
import shutil
import hashlib
from collections import namedtuple
import pymongo

# from xicam.core.msg import WARNING, notifyMessage, logError, logMessage
# from xicam.plugins.datahandlerplugin import DataHandlerPlugin
# import dask.array as da
# import dask

logger = logging.getLogger('ingest_733')

FILE_SPEC_MAPPING ={'tiff': 'AD_TIFF', 'tif': 'AD_TIFF', 'jpg': 'JPEG', 'jpeg': 'JPEG', 'edf': 'EDF'}

def ingest_733(raw_metadata, output_root, thumbnail_medadatas=None):
    yield from _createDocument(raw_metadata, output_root, thumbnail_medadatas)


ImageMetadata = namedtuple('ImageMetadata',
                           'hash path shape field_name file_ext')

def map_spec_from_file_ext(file_type):
    return FILE_SPEC_MAPPING.get(file_type.lower())

class ETLExecutor():

    def __init__(self, input_root, output_root):
        """init
        Parameters
        ----------
        input_root : str
            the root where source files live
        output_root : str
            throroot where processed files will be written to
        """
        self.input_root = input_root
        self.output_root = output_root

    def execute(self, path, formats=None):
        """Function for processing a number
        of files in a directory to get them ready
        for ML processing. 

        Includes multiple processing steps, including, for each file:

        1. Calculates a checksum of the file. This checksum is then used
            in the name of the file in the new directory.
        2. Using the relative path of the source file, parses
            the name of its folder and hashes the path. This becomes
            the folder that the file and all created files live in
            in the output directory. Thus a relative structure is maintained,
            but anonymizing the metadata that might be found in the path.
            This also lets us preserve groups of files in the output.
        3. Creates one file for each format typle that is passed.
        Example:

        If you have a file:  /foo/bar/great_image.edf
        And you pass it in with [('128', 'jpg'), ('256', 'tiff')]
        And the input_root is /foo/bar
        And the output_root is /processed/files/

        Then your would end up with the following (made up hashes for the example):
        /processed/files/fa2223asad444a/12af23444fcb34.edf
        /processed/files/fa2223asad444a/12af23444fcb34_128.jpg
        /processed/files/fa2223asad444a/12af23444fcb34_256.tiff

        Parameters
        ----------
        path : str
            path of the file to be processed
        formats : list of tuple of str, optional
            each tuple contains a combination of size and type to be converted,
            by default None
        
        Returns
        -------
        str
            path to the renamed source file
        
        list of str
            paths to each converted file

        Raises
        ------
        TypeError
            raised if a conversion format is not supported
        """

        for _, file_type in formats:
            if file_type not in ['tif', 'tiff', 'jpeg', 'jpg']:
                raise TypeError(f'type not supported: {file_type}')
        relative_path = os.path.relpath(path, self.input_root)
        relative_path_hash = self._hash_string(os.path.dirname(relative_path))
        file_payload_hash = self._hash_file(path)
        new_file_ext = os.path.splitext(path)[-1]

        dest_folder = os.path.join(self.output_root, relative_path_hash)
        os.makedirs(dest_folder, exist_ok=True)
        raw_file_path = os.path.join(
                        dest_folder, 
                        file_payload_hash + new_file_ext)
        shutil.copyfile(path, raw_file_path)
        thumbnail_metadatas = []
        raw_img = fabio.open(raw_file_path)
        raw_file_name = os.path.normpath(os.path.split(raw_file_path)[-1])
        raw_metadata = ImageMetadata(
            file_payload_hash,
            raw_file_path,
            raw_img.data.shape,
            'raw', 
            new_file_ext[1:])
        for format in formats:
            thumbnail_metadatas.append(self._convert_raw(
                                       raw_img,
                                       raw_file_name,
                                       dest_folder,
                                       *format))
        return raw_metadata, thumbnail_metadatas

    def _convert_raw(self, raw_img, raw_file_name, processed_path_root, size, file_type):
        log_image = np.array(raw_img.data)
        log_image = log_image - np.min(log_image) + 1.001
        log_image = np.log(log_image)
        log_image = 205*log_image/(np.max(log_image))
        auto_contrast_image = Image.fromarray(np.uint8(log_image))
        auto_contrast_image = ImageOps.autocontrast(
                                auto_contrast_image, cutoff=0.1)
        auto_contrast_image = resize(np.array(auto_contrast_image),
                                             (size, size))
        file_name = os.path.splitext(raw_file_name)[0]
        thumbnail_path = os.path.join(processed_path_root, '%s_%s.%s' %
                                      (file_name, size, file_type))
        imageio.imwrite(thumbnail_path,
                        auto_contrast_image)
        thumbnail_metadata = ImageMetadata(
            None,
            thumbnail_path,
            (size, size),
            str(size) + "_" + file_type,
            file_type)
        return thumbnail_metadata

    def _hash_file(self, path):
        BLOCKSIZE = 65536
        hasher = hashlib.md5()
        with open(path, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        return hasher.hexdigest()

    def _hash_string(self, s):
        hasher = hashlib.md5()
        hasher.update(s.encode('utf-8'))
        return hasher.hexdigest()


def _createDocument(raw_metadata: ImageMetadata,
                    output_root, 
                    thumbnail_metadatas: list = None):

    timestamp = time.time()
    # start document
    run_bundle = event_model.compose_run(uid=raw_metadata.hash)
    yield 'start', run_bundle.start_doc

    # event descriptor document for raw
    source = 'ALS 733'  # TODO -- find embedded source info?
    raw_data_keys = {'raw': {'source': source, 'dtype': 'number', 
                             'shape': [raw_metadata.shape[0], 
                                         raw_metadata.shape[1]]}}
    raw_stream_name = 'primary'
    raw_stream_bundle = run_bundle.compose_descriptor(
        data_keys=raw_data_keys,
        name=raw_stream_name)
    yield 'descriptor', raw_stream_bundle.descriptor_doc

    # resource for EDF
    edf_resource = run_bundle.compose_resource(
        spec=map_spec_from_file_ext(raw_metadata.file_ext),
        root=output_root,
        resource_path=os.path.relpath(raw_metadata.path, output_root),
        resource_kwargs={})
    yield 'resource', edf_resource.resource_doc

    # datum for EDF 
    datum = edf_resource.compose_datum(datum_kwargs={})
    yield 'datum', datum
    
    # event for EDF
    yield 'event', raw_stream_bundle.compose_event(
        data={raw_metadata.field_name: datum['datum_id']},
        timestamps={raw_metadata.field_name: timestamp})

    # event descriptor document for thumbnail
    source = 'ALS 733'

    # iterate over all the thumbnails to create keys for each
    thumbnail_data_keys = {thumnbnail_metadata.field_name: 
                       {'source': source, 'dtype': 'number', 'shape':
                        [thumnbnail_metadata.shape[0],
                         thumnbnail_metadata.shape[1]]} 
                       for thumnbnail_metadata in thumbnail_metadatas} 
    thumbnail_stream_name = 'thumbnails'
    thumbnail_stream_bundle = run_bundle.compose_descriptor(
        data_keys=thumbnail_data_keys,
        name=thumbnail_stream_name)
    yield 'descriptor', thumbnail_stream_bundle.descriptor_doc


    thumbnail_data = {}
    thumbnail_timestamps = {}
    # for thumbnail_path in thumbnail_paths:
    for thumbnail_metadata in thumbnail_metadatas:
        # each thumbnail resource
        thumbnail_resource = run_bundle.compose_resource(
            spec=map_spec_from_file_ext(thumbnail_metadata.file_ext),
            root=output_root,
            resource_path=os.path.relpath(thumbnail_metadata.path, output_root),
            resource_kwargs={})
        yield 'resource', thumbnail_resource.resource_doc
        
        #thumbnail datum
        datum = thumbnail_resource.compose_datum(datum_kwargs={})
        yield 'datum', datum

        thumbnail_data[thumbnail_metadata.field_name] = datum['datum_id']
        thumbnail_timestamps[thumbnail_metadata.field_name] = timestamp

    # event for EDF
    yield 'event', thumbnail_stream_bundle.compose_event(
        data=thumbnail_data,
        timestamps=thumbnail_timestamps)

    yield 'stop', run_bundle.compose_stop()

def add_to_tag_db(db, image_metadata: ImageMetadata):
    # http://learnmongodbthehardway.com/schema/metadata/

    tag_doc = {"source_id": image_metadata.hash,
               "image_suffix": image_metadata.field_name,
               "tags": [{"key": "", "value": "", "confidence": 0.0}]}

def create_tags_collection(db):
    tags_collection = db['tags']
    # tags_collection.create_index([])

def main():
    import glob
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
    # from suitcase.msgpack import export
    from suitcase.jsonl import export
    from suitcase.mongo_embedded import Serializer
    input_root = os.getenv('input_root')
    output_root = os.getenv('output_root')
    # output_root = '/tmp/beamlines/733/tagcam2'
    msg_pack_dir = os.getenv('msg_pack_dir')
    paths = glob.glob(os.getenv('input_relative'))
    logger.info(paths)
    etl_executor = ETLExecutor(input_root, output_root)
    for file_path in paths:
        try:
            raw_metdata, thumb_metadatas = etl_executor.execute(
                                                file_path, [(256, 'jpg'), (256, 'tiff')])
        except TypeError as e:
            logger.error(e)
            raise e
        except Exception as e:
            logger.error(e)
            raise e
        else:
            docs = ingest_733(raw_metdata, output_root, thumb_metadatas)
            export(docs, msg_pack_dir)
            # with Serializer(directory, file_prefix, cls=cls, **kwargs) as serializer:
            #     for item in gen:
            #         serializer(*item)

    return serializer.artifacts
if __name__ == '__main__':
    main()