import logging
from typing import List, Optional
import graphene

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.graphql import GraphQLApp
from starlette.config import Config

from .query import (
    Query,
    # Mutation,
    context as query_context
)


from .model import (
    Dataset,
    Tag,
    TagSource,
    TaggingEvent
)

from .tag_service import TagService, Context

logger = logging.getLogger('splash_ml')


def init_logging():
    ch = logging.StreamHandler()
    # ch.setLevel(logging.INFO)
    # root_logger.addHandler(ch)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(SPLASH_LOG_LEVEL)


config = Config(".env")
MONGO_DB_URI = config("MONGO_DB_URI", cast=str, default="mongodb://localhost:27017/tagging")
SPLASH_DB_NAME = config("SPLASH_DB_NAME", cast=str, default="splash")
SPLASH_LOG_LEVEL = config("SPLASH_LOG_LEVEL", cast=str, default="INFO")

API_URL_PREFIX = "/api/v0"

init_logging()

app = FastAPI(    
    openapi_url="/api/splash_ml/openapi.json",
    docs_url="/api/splash_ml/docs",
    redoc_url="/api/splash_ml/redoc",)


svc_context = Context()


@app.on_event("startup")
async def startup_event():
    from pymongo import MongoClient
    logger.debug('!!!!!!!!!starting server')
    db = MongoClient(MONGO_DB_URI)
    tag_svc = TagService(db)
    svc_context.tag_svc = tag_svc
    query_context.tag_svc = tag_svc


app.add_route('/graphql', GraphQLApp(schema=graphene.Schema(query=Query)))  # , mutation=Mutation)))


class CreateResponseModel(BaseModel):
    uid: str


@app.post(API_URL_PREFIX + '/datasets', tags=['datasets'], response_model=CreateResponseModel)
def add_dataset(asset: Dataset):
    new_asset = svc_context.tag_svc.create_dataset(asset)
    return CreateResponseModel(uid=new_asset.uid)


@app.get(API_URL_PREFIX + '/datasets', tags=['datasets'])
def get_datasets(
    name: Optional[str] = None,
    uri: Optional[str] = None,
    tag_value: Optional[str] = None,
    skip: Optional[int] = 0,
    limit: Optional[int] = 10

) -> List[Dataset]:
    # turning tag_value into tags.value as we do below feels too kludgy
    return svc_context.tag_svc.find_datasets(skip, limit, name=name, uri=uri, **{"tags.value": tag_value})


@app.post(API_URL_PREFIX + '/datasets/{uid}/tags', tags=['datasets', 'tags'], response_model=CreateResponseModel)
def add_tags(uid: str, tags: List[Tag]):
    new_asset = svc_context.tag_svc.add_tags(tags, uid)
    return CreateResponseModel(uid=new_asset.uid)


@app.post(API_URL_PREFIX + '/tagsources', tags=['tag sources'], response_model=CreateResponseModel)
def add_tag_source(asset: TagSource):
    new_tagger = svc_context.tag_svc.create_tag_source(asset)
    return CreateResponseModel(uid=new_tagger.uid)


@app.get(API_URL_PREFIX + '/tagsources', tags=['tag sources'], response_model=List[TagSource])
def get_tag_sources():
    tag_sources = svc_context.tag_svc.find_tag_sources()
    return tag_sources



@app.post(API_URL_PREFIX + '/events', tags=['events'], response_model=CreateResponseModel)
def add_event(event: TaggingEvent):
    new_event = svc_context.tag_svc.create_event(event)
    return CreateResponseModel(new_event.uid)
