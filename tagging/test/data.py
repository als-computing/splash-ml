from datetime import datetime

from ..model import (
    Dataset,
    DatasetType,
    Tag,
    TagSource,
    TaggingEvent
)


def json_datetime(unix_timestamp):
    return datetime.utcfromtimestamp(unix_timestamp).isoformat() + "Z"


new_dataset = Dataset(**{
    "type": DatasetType.file,
    "uri": "images/test.tiff",
    "tags": [
        {
            "name": "rods",
            "confidence": 0.9008,
            "event_id": None,
            "locator": {
                "spec": "test_locator",
                "path": ["foo", "bar"]
            }
        },
        {
            "name": "peaks",
            "confidence": 0.001,
            "locator": {
                "spec": "test_locator",
                "path": "simple path"
            }
        },
        {
            "name": "reflection",
            "confidence": 1,
            "event_id": "import_id1",
        }
    ]
})


# name didnt change, though it doesnt carry model_name anymore
new_tagging_event = TaggingEvent(**{
    "tagger_id": "Tricia McMillin",
    "run_time": json_datetime(1134433.223),
    "accuracy": 0.7776
})


new_tagger = TagSource(**{
    "type": "model",
    "name": "PyTestNet",
    "model_info": {
        "label_index": {
            "blue": 1,
            "red": 2
        }
    }
})

no_tag_dataset = Dataset(**{
    "type": "file",
    "uri": "blahblahblah"
})


no_tag = Tag(**{
    "name": "rod",
    "tags": None
})
