import jsonschema
import datetime


def json_datetime(unix_timestamp):
    return datetime.datetime.utcfromtimestamp(unix_timestamp).isoformat() + "Z"


example_asset_tags = {
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
}

schema_asset_tags = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Asset Tags Schema",
    "type": "object",
    "description": "v1",
    "required": [
        "uid", "sample_id", "tags"
    ],
    "optional": [
        "schema_version"  # TODO - is this necessary?
    ],
    "additionalProperties": False,
    "properties": {
        "uid": {
            "type": "string"
        },
        "sample_id": {
            "type": "string"
        },
        "tags": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "tag", "confidence", "event_id"
                ],
                "additionalProperties": False,
                "properties": {
                    "tag": {
                        "type": "string",
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                        "description": "must be between 0 and 1"
                    },
                    "event_id": {
                        "type": "string"
                    }
                }
            }
        },
        "schema_version": {
            "type": "string"
        }
    }
}

validator = jsonschema.Draft7Validator(schema=schema_asset_tags, format_checker=jsonschema.draft7_format_checker)
validator.validate(example_asset_tags)

example_tagging_event = {
    "uid": "wwewere6002104rwerwe81ad229c33",
    "tagger_id": "dsfosf9080s9uflkwsnerl3k4j",
    "run_time": json_datetime(1134433.223),
    "accuracy": 0.7776
}

schema_tagging_event = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Tagging Event Schema",
    "type": "object",
    "description": "v1",
    "required": [
        "uid", "tagger_id", "run_time", "accuracy"
    ],
    "optional": [
        "schema_version"
    ],
    "additionalProperties": False,
    "properties": {
        "uid": {
            "type": "string"
        },
        "tagger_id": {
            "type": "string"
        },
        "run_time": {
            "type": "string",
            "format": "date-time"  # TODO : maybe change tag service code to convert to ISO 8601
        },
        "accuracy": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0,
            "description": "must be between 0 and 1"  # TODO: Does it really have to be though?
        },
        "schema_version": {
            "type": "string"
        }
    }
}

validator = jsonschema.Draft7Validator(schema=schema_tagging_event, format_checker=jsonschema.draft7_format_checker)
validator.validate(example_tagging_event)

example_tagger = {
    "type": "model",
    "model_name": "PyTestNet",
    "uid": "dsfosf9080s9uflkwsnerl3k4j",
    "create_time": json_datetime(11112333.3)
}

schema_tagger = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Tagger Schema",
    "type": "object",
    "description": "v1",
    "required": [
        "type", "model_name", "uid", "create_time"
    ],
    "optional": [
        "schema_version"
    ],
    "additionalProperties": False,
    "properties": {
        "type": {
            "enum": ["model", "human"],  # tentative
            "description": "tagged either by an ML model or manually by a human"
        },
        "model_name": {
            "type": "string"
        },
        "uid": {
            "type": "string"
        },
        "create_time": {
            "type": "string",
            "format": "date-time"
        },
        "schema_version": {
            "type": "string"
        }
    },
}

validator = jsonschema.Draft7Validator(schema=schema_tagger, format_checker=jsonschema.draft7_format_checker)
validator.validate(example_tagger)
