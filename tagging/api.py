from enum import Enum
import logging
from typing import List, Optional
import graphene

from fastapi import FastAPI, Query as FastQuery, HTTPException
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
    UserDataset,
    Tag,
    TagSource,
    TaggingEvent,
    TagPatchRequest
)

from .tag_service import TagService, Context

logger = logging.getLogger('splash_ml')


DEFAULT_PAGE_SIZE = 20

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

class CreateTagPatchResponse(BaseModel):
    added_tags_uid: Optional[List[str]]
    removed_tags_uid: Optional[List[str]]

@app.post(API_URL_PREFIX + '/datasets', tags=['datasets'], response_model=CreateResponseModel)
def add_dataset(user_asset: UserDataset):
    user_asset_dic = user_asset.dict()
    asset = Dataset(**user_asset_dic)
    new_asset = svc_context.tag_svc.create_dataset(asset)
    return CreateResponseModel(uid=new_asset.uid)


@app.get(API_URL_PREFIX + '/datasets', tags=['datasets'])
def get_datasets(
    uri: Optional[str] = None,
    tags: Optional[List[str]] = FastQuery(None),
    offset: Optional[int] = FastQuery(0, alias="page[offset]"),
    limit: Optional[int] = FastQuery(DEFAULT_PAGE_SIZE, alias="page[limit]")

) -> List[Dataset]:


    """ Searches datasets based on query parameters. Provides pagine through skip and limit

    Args:
        uri (Optional[str], optional): find dataset based on uri. Defaults to None.
        tags(Optional[List[str]], optional): lsit of tags to search for. Defaults to none.
        skip (Optional[int], optional): [description]. Defaults to 0.
        limit (Optional[int], optional): [description]. Defaults to 10.

    Returns:
        List[Dataset]: [Full object datasets corresponding to search prarmeters]s
    """
    return svc_context.tag_svc.find_datasets(offset=offset, limit=limit, uri=uri, tags=tags)

@app.patch(API_URL_PREFIX + '/datasets/{uid}/tags', tags=['datasets', 'tags'], response_model=CreateTagPatchResponse)
def modify_tags(uid: str, req: TagPatchRequest):
    added_tags_uid = svc_context.tag_svc.add_tags(req.add_tags, uid)
    removed_tags_uid = svc_context.tag_svc.delete_tags(req.remove_tags, uid)
    return CreateTagPatchResponse(added_tags_uid=added_tags_uid, removed_tags_uid=removed_tags_uid)


@app.patch(API_URL_PREFIX + '/datasets/{uid}/metadata', tags=['datasets', 'metadata'], response_model=CreateResponseModel)
def add_tags(uid: str, tags: List[Tag]):
    raise HTTPException(405, detail="support for patching metadata is future")
    # new_asset = svc_context.tag_svc.add_metadata(tags, uid)
    # return CreateResponseModel(uid=new_asset.uid)


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

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)