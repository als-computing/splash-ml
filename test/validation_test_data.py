import datetime


def json_datetime(unix_timestamp):
    return datetime.datetime.utcfromtimestamp(unix_timestamp).isoformat() + "Z"


asset_tags_good = (
    {
        "uid": "ee600210432b8f81ad229c33",
        "sample_id": "house paint 1234",
        "tags": [
            {
                "tag": "rods",
                "confidence": 0.9008,
                "event_id": "wwewere6002104rwerwe81ad229c33"
            },
            {
                "tag": "peaks",
                "confidence": 0.001,
                "event_id": "wwewere6002104rwerwe81ad229c33"
            }
        ]
    },
    {
        "uid": " ",
        "sample_id": " ",
        "tags": [
        ]
    },
    {
        "uid": "lbl",
        "sample_id": "als",
        "tags": [
            {
                "tag": "froot loops",
                "confidence": 0,
                "event_id": "cheerios"
            },
            {
                "tag": " ",
                "confidence": 1,
                "event_id": " "
            }
        ]
    }
)
tagging_events_good = (
    {
        "uid": "wwewere6002104rwerwe81ad229c33",
        "tagger_id": "dsfosf9080s9uflkwsnerl3k4j",
        "run_time": json_datetime(1134433.223),
        "accuracy": 0.7776
    },
    {
        "uid": "vroom vroom",
        "tagger_id": "fast car",
        "run_time": json_datetime(1435677832),
        "accuracy": 0.01345678657865467865435678
    },
    {
        "uid": "good",
        "tagger_id": "accurate",
        "run_time": json_datetime(-56347824),
        "accuracy": 1
    },
    {
        "uid": "bad",
        "tagger_id": "inaccurate",
        "run_time": json_datetime(0),
        "accuracy": 0
    }
)
taggers_good = (
    {
        "type": "model",
        "model_name": "PyTestNet",
        "uid": "dsfosf9080s9uflkwsnerl3k4j",
        "create_time": json_datetime(11112333.3)
    },
    {
        "type": "human",
        "model_name": "Hari",
        "uid": "1111111111111",
        "create_time": '2020-08-02T08:59:28-07:00'
    },
    {
        "type": "model",
        "model_name": "   ",
        "uid": "   ",
        "create_time": json_datetime(-5)
    }
)


asset_tags_bad = (
    {
        "uid": "10",
        "sample_id": "......",
        "tags": [
            {
                "tag": "rods",
                "confidence": 15,
                "event_id": "wwewere6002104rwerwe81ad229c33"
            },
            {
                "tag": "peaks",
                "confidence": 0.001,
                "event_id": "wwewere6002104rwerwe81ad229c33"
            }
        ]
    },
    {
        "uid": " ",
        "sample_id": " "
    },
    {
        "uid": "lbl",
        "sample_id": "als",
        "tags": [
            {
                "tag": "froot loops",
                "confidence": -5,
                "event_id": "cheerios"
            },
            {
                "tag": " ",
                "confidence": 1,
                "event_id": " "
            }
        ]
    },
    {
        "uid": "lbl",
        "sample_id": "als",
        "tags": [
            {
                "tag": "froot loops",
                "event_id": "cheerios"
            },
            {
                "tag": " ",
                "confidence": 1,
                "event_id": " "
            }
        ]
    },
    {
        "uid": "lbl",
        "sample_id": "als",
        "tags": [
            {
                "tag": "froot loops",
                "confidence": 0.5
            },
            {
                "tag": " ",
                "confidence": 1,
                "event_id": " "
            }
        ]
    },
    {
        "uid": "lbl",
        "sample_id": "als",
        "tags": [
            {
                "tag": "froot loops",
                "confidence": 0.5,
                "event_id": "10"
            },
            {
                "tag": " ",
                "confidence": 1,
            }
        ]
    }
)
tagging_events_bad = (
    {
        "uid": 2323244443,
        "tagger_id": "dsergrgerg",
        "run_time": 242242424244,
        "accuracy": 0.7776
    },
    {
        "uid": "good id",
        "tagger_id": "good tagger id",
        "run_time": json_datetime(1435677832),
    },
    {
        "uid": "dummy",
        "tagger_id": 224242422,
        "run_time": json_datetime(2) * 5,
        "accuracy": 1
    },
    {
        "uid": "dummy2",
        "tagger_id": "224242422",
        "run_time": json_datetime(3456778) + "QWERTY",
        "accuracy": 7
    }
)
taggers_bad = (
    {
        "model_name": "PyTestNet",
        "uid": "dsfosf9080s9uflkwsnerl3k4j",
        "create_time": json_datetime(11112333.3)
    },
    {
        "type": "frog",
        "model_name": "Kermit",
        "uid": "1111111111111",
        "create_time": json_datetime(11112333.3)
    },
    {
        "type": "model",
        "model_name": "   ",
        "uid": "   ",
        "create_time": 5673812.3
    }
)
