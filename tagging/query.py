from tagging.tag_service import TagService
import graphene
from graphene_pydantic import PydanticInputObjectType, PydanticObjectType

from .model import (
    Tagger,
    NewTagger
)


class QueryContext():
    tag_svc: TagService = None


context = QueryContext()


class TaggerGrapheneInputModel(PydanticInputObjectType):
    class Meta:
        model = NewTagger


class TaggerGrapheneModel(PydanticObjectType):
    class Meta:
        model = Tagger


class CreateTagger(graphene.Mutation):
    class Arguments:
        tagger_details = TaggerGrapheneInputModel()

    Output = TaggerGrapheneModel

    @staticmethod
    def mutate(parent, info, tagger_details):
        context.tag_svc.create_tagger(NewTagger(**tagger_details))


class Mutation(graphene.ObjectType):
    create_tagger = CreateTagger.Field()


class Query(graphene.ObjectType):   
    list_taggers = graphene.List(TaggerGrapheneModel)
    list_taggers_by_type = graphene.List(TaggerGrapheneModel, args={"type": graphene.String(), "name": graphene.String()})

    @staticmethod
    def resolve_list_taggers(parent, info):
        taggers = context.tag_svc.get_taggers()
        return taggers

    @staticmethod
    def resolve_list_taggers_by_type(parent, info, type, name):
        taggers = context.tag_svc.get_taggers(type=type)
        return taggers
