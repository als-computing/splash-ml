# Tag Service Data Model
The Tag Service persists data in Mongodb. Here we list information about the data model.




## Example Documents

#### Dataset (current)
A data set stores information about a dataset like location, tags, and other metadata. It does not store the data set itself.
``` json
{
    "uid": "ee600210432b8f81ad229c33",
    "type": "image",
    "asset_locator": {
        "uid": "sdlkjfsl3l23kj9s80jl",
        "type": "file",
        "locator": "/a/file.png"
    },
    "sample_id": "house paint 1234",
    "tags": [
        {
            "name": "label",
            "locator": { "min": 1, "max": 2},
            "value": "rods",
            "confidence": 0.900,
            "event_id": "wwewere6002104rwerwe81ad229c33",
        },
        {
            "name": "label",
            "value": "peaks",
            "locator": { "min": 1, "max": 2},
            "confidence": 0.001, 
            "event_id": "wwewere6002104rwerwe81ad229c33",
        },
            {
            "name": "geometry",
            "value": "GISAXS",
            "locator": { "min": 1, "max": 2},
            "confidence": 1.0, 
            "event_id": "wwewere6002104rwerwe81ad229c33",
        }
    ],
}
```

### Dataset (Proposed)
A data set stores information about a dataset like location, and other metadata. It does not store the data set itself.
``` json
{
    "uid": "ee600210432b8f81ad229c33",
    "type": "image",
    "asset_locator": {
        "uid": "sdlkjfsl3l23kj9s80jl",
        "type": "file",
        "locator": "/a/file.png"
    },
    "sample_id": "house paint 1234",
    "tags": [
        
    ],
}
```

### Tags (proposed)
``` json
{
    "dataset_id": "ee600210432b8f81ad229c33",
    "locator": { "min": 1, "max": 2},
    "name": "label",
    "value": "rods",
    "confidence": 0.900,
    "event_id": "wwewere6002104rwerwe81ad229c33",
},
{
    "dataset_id": "ee600210432b8f81ad229c33",
    "locator": { "min": 1, "max": 2},
    "name": "label",
    "value": "peaks",
    "confidence": 0.001, 
    "event_id": "wwewere6002104rwerwe81ad229c33",
},
{
    "dataset_id": "ee600210432b8f81ad229c33",
    "locator": { "min": 1, "max": 2},
    "name": "geometry",
    "value": "GISAXS",
    "confidence": 1.0, 
    "event_id": "wwewere6002104rwerwe81ad229c33",
}

```


### Tagging Event
``` json
{
    "uid": "wwewere6002104rwerwe81ad229c33",
    "tagger_id": "dsfosf9080s9uflkwsnerl3k4j",
    "run_time": 1134433.223,
    "accuracy": 0.7776
}

```

### Tagger
``` json
    {
    "type": "model",
    "model_name": "PyTestNet",
    "uid": "dsfosf9080s9uflkwsnerl3k4j",
    "create_time": 11112333.3,
    "model_info": {
        "label_index": {
            "blue": 1,
            "red": 2
        }
    }
}
```