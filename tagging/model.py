from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Extra, Field, create_model
from typing import Dict, List, Optional, Tuple, Any, Union, Type

# https://www.mongodb.com/blog/post/building-with-patterns-the-schema-versioning-pattern
SCHEMA_VERSION = "1.2"
DEFAULT_UID = "342e4568-e23b-12d3-a456-526714178000"


class Persistable(BaseModel):
    uid: Optional[str]


class ModelInfo(BaseModel):
    label_index: Optional[Dict[str, float]]


class NVPair(BaseModel):
    name: str
    value: str
    scope: Optional[str]

class Metadata(BaseModel):
    properties: List[NVPair]



class SimpleMetadata(Metadata):
    pass


class TagSource(Persistable):
    schema_version: str = SCHEMA_VERSION
    model_info: Optional[ModelInfo]
    type: str
    name: Optional[str] = Field(description="optional name of model that produces tags")

    class Config:
        extra = Extra.forbid


class TaggingEvent(Persistable):
    schema_version: str = SCHEMA_VERSION
    tagger_id: str
    run_time: datetime
    accuracy: Optional[float] = Field(ge=0.0, le=1.0)

    class Config:
        extra = Extra.forbid


class Tag(BaseModel):
    uid: str = DEFAULT_UID
    name: str = Field(description="name of the tag")
    locator: Optional[str] = Field(description="optional location information, " \
                            "for indicating a part of a dataset that this tag applies to")
    confidence: Optional[float] = Field(description="confidence provided for this tag")
    event_id: Optional[str] = Field(description="id of event where this tag was created")


class DatasetType(str, Enum):
    dbroker = "dbroker"
    file = "file"
    web = "web"


class DatasetCollection(Persistable):
    assets: List[str]
    models: Dict[str, int] # model and the quality of that model when run against a model


class Dataset(BaseModel):
    uid: str = DEFAULT_UID
    schema_version: str = SCHEMA_VERSION
    type: DatasetType
    uri: str
    location_kwargs: Optional[Dict[str, str]]
    sample_id: Optional[str]
    tags: Optional[List[Tag]]
    metadata: Optional[Union[Metadata, SimpleMetadata]]
    
    class Config:
        extra = Extra.forbid

 
class FileDataset(Dataset):
    type = DatasetType.file


def add_to_metadata(model: Type[BaseModel]) -> Dict[str, Tuple[Any, None]]:
    """
    Generate `field_definitions` for `create_model` by taking fields from
    `model` and making them all optional (setting `None` as default value)
    """
    # return {f.name: (f.type_, None) for f in model.__fields__.values()}
    model_fields = {}
    for f in model.__fields__.values():
        if f.name == "metadata":
            f.type_ = Union[str, int]
        # model_fields[f.name] = (f.type_, f.default)
        model_fields[f.name] = f
    return model_fields

# CustomDataset = create_model('Dataset', **add_to_metadata(Dataset))
CustomDataset = create_model(
    'Dataset', 
    __config__ = Dataset.__config__,
    # __base__ = Dataset.__base__,
    __module__ = Dataset.__module__,
    __validators__ = Dataset.__validators__,
    **add_to_metadata(Dataset))

Dataset = CustomDataset
class TagPatchRequest(BaseModel):
    add_tags: Optional[List[Tag]]
    remove_tags: Optional[List[str]]

