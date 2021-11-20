from ariadne import ObjectType, QueryType, gql, make_executable_schema
from pymongo import MongoClient

from starlette.config import Config

from .tag_service import TagService


config = Config(".env")
MONGO_DB_URI = config("MONGO_DB_URI", cast=str, default="mongodb://localhost:27017/tagging")
SPLASH_DB_NAME = config("SPLASH_DB_NAME", cast=str, default="splash")
SPLASH_LOG_LEVEL = config("SPLASH_LOG_LEVEL", cast=str, default="INFO")

db = MongoClient(MONGO_DB_URI)
tag_svc = TagService(db)


type_defs = gql("""

    type Query {
        datasets(tags: [String], uri: String): [Dataset]!

    }


    enum DatasetType {
        tiled
        file
        web
    }

    type Tag {

        uid: String
        name: String!
        locator: String
        accuracy: Float
    }

    type TagSource{
        " The entity that created a Tag "
        type: String!
        name: String

    }

    type TaggingEvent {
        tagger: TagSource
    }

    type Dataset {
        " Dataset model "
        uri: String!
        type: DatasetType!
        tags: [Tag]
    }

""")

query = QueryType()

class DatasetResolver():

    def __init__(self, tag_svc):
        self._tag_svc = tag_svc

    @query.field("datasets")
    def __call__(self, *_, tags=None, uri=None):
        datasets = list(tag_svc.find_datasets(tags=tags, uri=uri))
        return datasets


dataset = ObjectType("Dataset")
schema = make_executable_schema(type_defs, query, dataset)
