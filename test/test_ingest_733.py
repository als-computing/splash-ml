from etl.ingest_733 import tagging_callback


def test_tagging_callback():
    path = '/foo/bar/beamstop/waxs/AgB000001/goobbledygook.edf'
    tags_dict = tagging_callback(path)
    assert len(tags_dict) == 4
    assert tags_dict['sample_detector_distance_name'] == 'waxs'
    assert tags_dict.get('scattering_geometry') is None
    assert tags_dict['beamline'] == 'beamstop'
    assert tags_dict['calibrant'] == 'agb'
