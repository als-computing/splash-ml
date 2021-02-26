import logging
from typing import List
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
    Asset,
    Tag,
    Tagger,
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


app.add_route('/graphql', GraphQLApp(schema=graphene.Schema(query=Query))) # , mutation=Mutation)))


class CreateResponseModel(BaseModel):
    id: str


@app.post('/api/v0/assets', tags=['assets'], response_model=CreateResponseModel)
def add_asset(asset: Asset):
    new_asset = svc_context.tag_svc.create_asset(asset)
    return CreateResponseModel(new_asset.uid)


@app.get('/api/v0/assets', tags=['assets'])
def get_assets() -> List[Asset]:
    return svc_context.tag_svc.find_assets()


@app.post('/api/v0/assets/{uid}/tags', tags=['assets', 'tags'], response_model=CreateResponseModel)
def add_tags(asset_id: str, tags: List[Tag]):
    new_asset = svc_context.tag_svc.add_tags(tags, asset_id)
    return CreateResponseModel(new_asset.uid)


@app.post('/api/v0/taggers', tags=['taggers'], response_model=CreateResponseModel)
def add_tagger(asset: Tagger):
    new_tagger = svc_context.tag_svc.create_tagger(asset)
    return CreateResponseModel(new_tagger.uid)


@app.post('/api/v0/events', tags=['events'], response_model=CreateResponseModel)
def add_event(event: TaggingEvent):
    new_event = svc_context.tag_svc.create_event(event)
    return CreateResponseModel(new_event.uid)
