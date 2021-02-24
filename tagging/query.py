from tagging.tag_service import TagService
import graphene
from graphene_pydantic import PydanticInputObjectType, PydanticObjectType
from .model import (
    Asset,
    LABEL_NAME,
    Tag,
    Tagger
)


class QueryContext():
    tag_svc: TagService = None


context = QueryContext()


class TaggerGrapheneInputModel(PydanticInputObjectType):
    class Meta:
        model = Tagger
        exclude_fields = ("modelinfo",)


class TaggerGrapheneModel(PydanticObjectType):
    class Meta:
        model = Tagger
        exclude_fields = ("modelinfo",)


class AssetGrapheneModel(PydanticObjectType):

    class Meta:
        model = Asset
        exclude_fields = ("location_kwargs", "tags")


class TagGrapheneModel(PydanticObjectType):

    class Meta:
        model = Tag


class TagGrapheneModel(PydanticObjectType):

    class Meta:
        model = Tag


class CreateTagger(graphene.Mutation):
    class Arguments:
        tagger_details = TaggerGrapheneInputModel()

    Output = TaggerGrapheneModel

    @staticmethod
    def mutate(parent, info, tagger_details):
        context.tag_svc.create_tagger(Tagger(**tagger_details))


class Mutation(graphene.ObjectType):
    create_tagger = CreateTagger.Field()


class Query(graphene.ObjectType):
    list_taggers = graphene.List(TaggerGrapheneModel)
    list_taggers_by_type = graphene.List(TaggerGrapheneModel, args={"type": graphene.String(), "name": graphene.String()})
    list_assets_by_tag = graphene.List(AssetGrapheneModel, args={"label": graphene.String()})

    @staticmethod
    def resolve_list_taggers(parent, info):
        taggers = context.tag_svc.get_taggers()
        return taggers

    @staticmethod
    def resolve_list_taggers_by_type(parent, info, type, name):
        taggers = context.tag_svc.get_taggers(type=type)
        return taggers

    @staticmethod
    def resolve_list_assets_by_tag(parent, info, label):
        assets = context.tag_svc.find_assets(**{"tags.value": label})
        return list(assets)
