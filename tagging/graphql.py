from ariadne import ObjectType, QueryType, gql, make_executable_schema

from .tag_service import TagService

type_defs = gql("""

    type Query {
        datasets(uris: [String], tags: [String], limit: Int, skip: Int): [Dataset]!
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
        confidence: Float
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
        uid: String!
        uri: String!
        type: DatasetType!
        tags: [Tag]
    }

""")

query = QueryType()


def set_gql_tag_service(new_tag_svc: TagService):
    global tag_svc
    tag_svc = new_tag_svc


@query.field("datasets")
def resolve_datasets(self, *_, tags=None, uris=None, limit=10, skip=0):
    datasets = list(tag_svc.find_datasets(tags=tags, uris=uris, offset=skip, limit=limit))
    return datasets


dataset = ObjectType("Dataset")
schema = make_executable_schema(type_defs, query)
