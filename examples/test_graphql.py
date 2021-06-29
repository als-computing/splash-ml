import uuid
from typing import Dict

import pydantic
import graphene
from graphene_pydantic import PydanticObjectType

from pprint import pprint


class AddressModel(pydantic.BaseModel):
    street: str
    city: str


class PersonModel(pydantic.BaseModel):
    id: uuid.UUID
    first_name: str
    last_name: str
    address: AddressModel


class Address(PydanticObjectType):
    class Meta:
        model = AddressModel


class Person(PydanticObjectType):
    class Meta:
        model = PersonModel


class Query(graphene.ObjectType):
    people = graphene.List(Person)

    @staticmethod
    def resolve_people(parent, info):
        # fetch actual PersonModels here
        return [PersonModel(id=uuid.uuid4(), first_name="Beth", last_name="Smith")]


# schema = graphene.Schema(query=Query)
# pprint(schema.graphql_schema)
# query = """
#     query {
#       people {
#         firstName,
#         lastName
#       }
#     }
# """
# result = schema.execute(query)
# print(result.data['people'][0])
