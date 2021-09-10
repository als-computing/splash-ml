from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Extra, Field, parse_obj_as
from typing import Dict, List, Optional
from uuid import UUID, uuid4
from random import getrandbits

# https://www.mongodb.com/blog/post/building-with-patterns-the-schema-versioning-pattern
SCHEMA_VERSION = "1.1"


def new_uuid():
    return str(uuid4())


class Persistable(BaseModel):
    uid: Optional[str]


class ModelInfo(BaseModel):
    label_index: Optional[Dict[str, float]]


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
    name: str = Field(description="name of the tag")
    locator: Optional[str] = Field(description="optional location information, " \
                            "for indicating a part of a dataset that this tag applies to")
    confidence: Optional[float] = Field(description="confidence provided for this tag")
    event_id: Optional[str] = Field(description="id of event where this tag was created")


class PersistedTag(Tag, Persistable):
    uid: str = Field(default_factory=new_uuid)
    pass


class DatasetType(str, Enum):
    dbroker = "dbroker"
    file = "file"
    web = "web"


class DatasetCollection(Persistable):
    assets: List[str]
    models: Dict[str, int] # model and the quality of that model when run against a model


class Dataset(BaseModel):
    schema_version: str = SCHEMA_VERSION
    type: DatasetType
    uri: str
    location_kwargs: Optional[Dict[str, str]]
    sample_id: Optional[str]
    tags: Optional[List[Tag]]

    class Config:
        extra = Extra.forbid


class PersistedDataset(Dataset, Persistable):
    uid: str = Field(default_factory=new_uuid)
    tags: Optional[List[PersistedTag]]

    def __init__(self, **data):
        tags = data.get('tags')
        # transforms a user defined tag into a splash-ml tag
        if tags is not None:
            data.pop('tags')
            super().__init__(**data)
            tag_list = parse_obj_as(List[PersistedTag], tags)
            self.tags = tag_list
        else:
            super().__init__(**data)

 
class FileDataset(Dataset):
    type = DatasetType.file


class TagPatchRequest(BaseModel):
    add_tags: Optional[List[Tag]]
    remove_tags: Optional[List[str]]

