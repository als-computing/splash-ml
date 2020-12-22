import os
from pathlib import Path
from tagging.util.files import anonymize_copy


def test_file_anonymize_copy(tmpdir): 
    rel_file = os.path.join("one", "two", "test.tiff")
    src_file = Path(__file__).parent / rel_file
    new_file = anonymize_copy(src_file, 'two', tmpdir)
    assert os.path.exists(new_file), "file was copied"
    assert tmpdir.dirname in new_file, "new file should have the tempdir name in it"
    assert os.path.splitext(new_file)[1] == ".tiff", "new file should be a .tiff"
