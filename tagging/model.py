from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Extra, Field
from typing import Dict, List, Optional

LABEL_NAME = "label"

# https://www.mongodb.com/blog/post/building-with-patterns-the-schema-versioning-pattern
SCHEMA_VERSION = "1.0"


class NewTagger(BaseModel):
    schema_version: str = SCHEMA_VERSION
    type: str
    name: Optional[str] = Field(description="optional name of model that produces tags")

    class Config:
        extra = Extra.forbid


class Tagger(NewTagger):
    uid: str


class NewTaggingEvent(BaseModel):
    schema_version: str = SCHEMA_VERSION
    tagger_id: str
    run_time: datetime
    accuracy: Optional[float] = Field(ge=0.0, le=1.0)

    class Config:
        extra = Extra.forbid


class Tag(BaseModel):
    name: str
    value: str
    confidence: Optional[float] = Field(ge=0.0, le=1.0)
    event_id: Optional[str] = None


class TaggingEvent(NewTaggingEvent):
    uid: str


class LocatorType(str, Enum):
    dbroker = "dbroker"
    file = "file"
    web = "web"


class AssetLocator(BaseModel):
    uid: str
    type: LocatorType
    locator: str
    kwargs: Optional[Dict[str, str]]


class NewAsset(BaseModel):
    schema_version: str = SCHEMA_VERSION
    type: Optional[str]
    sample_id: Optional[str]
    tags: Optional[List[Tag]]
    asset_locator: Optional[AssetLocator]

    class Config:
        extra = Extra.forbid


class Asset(NewAsset):
    uid: str
