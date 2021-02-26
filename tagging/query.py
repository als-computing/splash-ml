from graphene.types.objecttype import ObjectType
from tagging.tag_service import TagService
import graphene
from graphene_pydantic import PydanticInputObjectType, PydanticObjectType
from .model import (
    Asset as AssetModel,
    LABEL_NAME,
    Tag as TagModel,
    Tagger as TaggerModel
)


class QueryContext():
    tag_svc: TagService = None


context = QueryContext()


class TaggerInput(PydanticInputObjectType):
    class Meta:
        model = TaggerModel
        exclude_fields = ("modelinfo",)


class Tagger(PydanticObjectType):
    class Meta:
        model = TaggerModel
        exclude_fields = ("modelinfo",)


class Tag(PydanticObjectType):

    class Meta:
        model = TagModel


class Asset(PydanticObjectType):
    # tags = graphene.List(of_type=Tag)

    # def resolve_tags(parent, info):
    #     return graphene.List(parent)

    class Meta:
        model = AssetModel
        exclude_fields = ("location_kwargs")



class Tag(PydanticObjectType):
    
    class Meta:
        model = TagModel


class CreateTagger(graphene.Mutation):
    class Arguments:
        tagger_details = TaggerInput()

    Output = Tagger

    @staticmethod
    def mutate(parent, info, tagger_details):
        context.tag_svc.create_tagger(TaggerModel(**tagger_details))


class Mutation(graphene.ObjectType):
    create_tagger = CreateTagger.Field()


class Query(graphene.ObjectType):
    list_taggers = graphene.List(Tagger)
    list_taggers_by_type = graphene.List(Tagger, args={"type": graphene.String(), "name": graphene.String()})
    list_assets_by_tag = graphene.List(Asset, args={"label": graphene.String()})

    @staticmethod
    def resolve_list_taggers(parent, info):
        taggers = context.tag_svc.find_taggers()
        return taggers

    @staticmethod
    def resolve_list_taggers_by_type(parent, info, type, name):
        taggers = context.tag_svc.find_taggers(type=type)
        return taggers

    @staticmethod
    def resolve_list_assets_by_tag(parent, info, label):
        assets = context.tag_svc.find_assets(**{"tags.value": label})
        return list(assets)
