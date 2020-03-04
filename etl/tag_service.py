from mongoengine import StringField, EmbeddedDocumentListField, Document, EmbeddedDocument, connect
from pymongo import MongoClient


class Tag(EmbeddedDocument):
    key = StringField()
    value = StringField()


class TaggableImage(Document):
    meta = {        
            'indexes': [
            ('tags.key',
             'tags.value')
            ]
    }
    uid = StringField(required=True, unique=True)
    relative_path = StringField(required=True)
    tags = EmbeddedDocumentListField(document_type=Tag)


class TagService():
    def __init__(self, db=None):
        self.db = db

    def create_image(self, image_uid, image_relative_path, tags_dict):
        # tag_objects = []
        # {tag_objects.append({"key": k, "value": v}) for (k, v) in tags.items()}
        # tags = EmbeddedDocumentListField()
        # tag = Tag(key='foo', value='bar')
        image = TaggableImage(
            uid=image_uid,
            relative_path=image_relative_path
        )
        for k, v in tags_dict.items():
            image.tags.append(Tag(key=k, value=v))
        image.save()