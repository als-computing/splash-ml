from graphene.types.objecttype import ObjectType
from tagging.tag_service import TagService
import graphene
from graphene_pydantic import PydanticInputObjectType, PydanticObjectType
from .model import (
    Dataset as DatasetModel,
    Tag as TagModel,
    TagSource as TagSourceModel
)


class QueryContext():
    tag_svc: TagService = None


context = QueryContext()


class TagSourceInput(PydanticInputObjectType):
    class Meta:
        model = TagSourceModel
        exclude_fields = ("model_info",)


class TagSource(PydanticObjectType):
    class Meta:
        model = TagSourceModel
        exclude_fields = ("model_info",)


class Tag(PydanticObjectType):

    class Meta:
        model = TagModel


class Dataset(PydanticObjectType):
    tags = graphene.List(of_type=Tag)

    def resolve_tags(parent, info):
        return graphene.List(parent)

    class Meta:
        model = DatasetModel
        exclude_fields = ("location_kwargs")





class CreateTagSource(graphene.Mutation):
    class Arguments:
        tagger_details = TagSourceInput()

    Output = TagSource

    @staticmethod
    def mutate(parent, info, tagger_details):
        context.tag_svc.create_tag_source(TagSourceModel(**tagger_details))


class Mutation(graphene.ObjectType):
    create_tag_source = CreateTagSource.Field()


class Query(graphene.ObjectType):
    list_taggers = graphene.List(TagSource)
    list_taggers_by_type = graphene.List(TagSource, args={"type": graphene.String(), "name": graphene.String()})
    list_assets_by_tag = graphene.List(Dataset, args={"label": graphene.String()})

    @staticmethod
    def resolve_list_taggers(parent, info):
        taggers = context.tag_svc.find_tag_sources()
        return taggers

    @staticmethod
    def resolve_list_taggers_by_type(parent, info, type, name):
        taggers = context.tag_svc.find_tag_sources(type=type)
        return taggers

    @staticmethod
    def resolve_list_assets_by_tag(parent, info, label):
        assets = context.tag_svc.find_assets(**{"tags.value": label})
        return list(assets)
