from pathlib import Path
import os
import pytest
import shutil
from etl.ingest import ETLExecutor

@pytest.fixture
def tmp_image_file(src_root):
    file = os.path.join(os.path.dirname(__file__), 'images', 'test.tiff')
    copied_file = os.path.join(src_root, 'test.tiff')
    
    shutil.copyfile(file, copied_file)
    return copied_file

@pytest.fixture
def src_root(tmpdir):
    root = Path(tmpdir, 'src')
    os.mkdir(root)
    return root


@pytest.fixture
def output_root(tmpdir):
    root = Path(tmpdir, 'out')
    os.mkdir(root)
    return root


def test_register_tagger(tmp_image_file, src_root, output_root):
    tags_dict = {"foo": "bar", "foo2": "bar2"}
    
    def my_tagging_callback(source_path):
        return tags_dict

    etl_executor = ETLExecutor(src_root, output_root, my_tagging_callback)
    _, _, return_tags = etl_executor.execute(tmp_image_file)
    assert return_tags == tags_dict

def test_no_formats(tmp_image_file, src_root, output_root):
    etl_executor = ETLExecutor(src_root, output_root)
    _, _, return_tags = etl_executor.execute(tmp_image_file)
