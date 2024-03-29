from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
# https://www.mongodb.com/blog/post/building-with-patterns-the-schema-versioning-pattern
SCHEMA_VERSION = "1.3"
DEFAULT_UID = "342e4568-e23b-12d3-a456-526714178000"


class Persistable(BaseModel):
    uid: Optional[str] = None


class ModelInfo(BaseModel):
    label_index: Optional[Dict[str, float]]


class Locator(BaseModel):
    spec: str = Field(description="Description of the specification for this locator")
    path: Any = Field(description="Locator information defined by the spec field")


class TagSource(Persistable, extra='forbid'):
    schema_version: str = SCHEMA_VERSION
    model_info: Optional[ModelInfo] = None
    type: str = None
    name: Optional[str] = Field(description="optional name of model that produces tags",
                                default=None)


class TaggingEvent(Persistable, extra='forbid'):
    schema_version: str = SCHEMA_VERSION
    tagger_id: str
    run_time: datetime
    accuracy: Optional[float] = Field(ge=0.0, le=1.0, default=None)


class Tag(BaseModel):
    uid: str = DEFAULT_UID
    name: str = Field(description="name of the tag")
    locator: Optional[Locator] = Field(description="optional location information, "
                                                   "for indicating a subset of a dataset that this "
                                                   "tag applies to",
                                       default=None)

    confidence: Optional[float] = Field(description="confidence provided for this tag",
                                        default=None)
    event_id: Optional[str] = Field(description="id of event where this tag was created",
                                    default=None)


class DatasetType(str, Enum):
    tiled = "tiled"
    file = "file"
    web = "web"


class DatasetCollection(Persistable):
    assets: List[str]
    models: Dict[str, int]  # model and the quality of that model when run against a model


class Dataset(BaseModel, extra='forbid'):
    uid: str = DEFAULT_UID
    schema_version: str = SCHEMA_VERSION
    project: Optional[str] = None
    type: DatasetType = None
    uri: str = None
    tags: Optional[List[Tag]] = None


class SearchDatasetsRequest(BaseModel):
    uris: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    project: Optional[str] = None
    event_id: Optional[str] = None


class TagPatchRequest(BaseModel):
    add_tags: Optional[List[Tag]] = None
    remove_tags: Optional[List[str]] = None
