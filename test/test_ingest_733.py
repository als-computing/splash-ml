from etl.ingest_733 import tagging_callback

def test_tagging_callback():
    path = '/foo/bar/giwaxs/goobbledygook.edf'
    tags_dict = tagging_callback(path)
    assert len(tags_dict) == 2
    assert tags_dict['scattering_geometry'] == 'reflection'
    assert tags_dict['sample_detector_distance_name'] == 'WAXS'