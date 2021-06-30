from typing import Iterator, List
import uuid

from pymongo.mongo_client import MongoClient

from .model import (
    Dataset,
    Tag,
    TagSource,
    TaggingEvent
)


class DatasetNotFound(Exception):
    pass




class TagService():
    """The TagService provides access to the tagging database
    as well as an interface into databroker which has ingested
    taggable files (both raw and thumbnailed)

    Usage looks something like:
    tag_svc = TagService(pymongo_client)
    tag_svc.create_tag_source(tagger)
    """

    def __init__(self, client, db_name=None):
        """Initialize a TagService entry using the
        With the provided pymongo.MongoClient instance, the
        service will create:
        - a database called 'tagging'
        - a collection called 'tagger'
        - a collection called 'tagging_event'
        - a collection called 'asset_tags'
        - relevant indexes

        Parameters
        ----------
        client : pymongo.MongoClient
            mongo client that service will use to connect to

        db_name : str
            optional mongo database name, default is 'tagging'

        root_catalog : intake.catalog.Catalog
            optional root catalog from which to query for
            run catalog
        """
        if db_name is None:
            db_name = 'tagging'
        self._db = client[db_name]
        self._collection_tag_sources = self._db.tag_source
        self._collection_tagging_event = self._db.tagging_event
        self._collection_dataset = self._db.data_set
        self._create_indexes()

    def create_tag_source(self, tag_source: TagSource) -> TagSource:
        """
        Create a new tagger data set. The uid from this tagger will act like a
        session for all create tagging_events in create_tagging_event
        Parameters
        ----------
        tagger : NewTagSource


        Returns
        ----------
        TagSource
            uid for this tagger, which can be added for all Tagging events
        """
        tagger_dict = tag_source.dict()
        self._inject_uid(tagger_dict)
        # tagger_dict['schema_version'] = self.SCHEMA_VERSION
        self._collection_tag_sources.insert_one(tagger_dict)
        self._clean_mongo_ids(tagger_dict)
        return TagSource(**tagger_dict)

    def create_tagging_event(self, event: TaggingEvent) -> TaggingEvent:
        """ Create a new tagging_event data set.  The uid for this tag event will label
        the event to create asset tags on.

        Parameters
        ----------
        tagging_event : NewTaggingEvent

        Returns
        ----------
        TaggingEvent
            TaggingEven object, with uid in it
        """
        event_dict = event.dict()
        self._inject_uid(event_dict)

        # event_dict['schema_version'] = self.SCHEMA_VERSION
        self._collection_tagging_event.insert_one(event_dict)
        self._clean_mongo_ids(event_dict)
        return TaggingEvent(**event_dict)

    def retrieve_tagging_event(self, uid: str) -> TaggingEvent:
        t_e_dict = self._collection_tagging_event.find_one({'uid': uid})
        self._clean_mongo_ids(t_e_dict)
        return TaggingEvent.parse_obj(t_e_dict)

    def create_dataset(self, asset: Dataset) -> Dataset:
        """ Create a new asset_tags data set.  The uid for this asset tag set
        distinguishes it from others and can be used to find it later.

        Parameters
        ----------
        asset_tags : NewDataset

        Returns
        ----------
        Dataset
            Dataset object, with uid in it
        """
        asset_dict = asset.dict()
        self._inject_uid(asset_dict)
        # asset_dict['schema_version'] = self.SCHEMA_VERSION
        self._collection_dataset.insert_one(asset_dict)
        self._clean_mongo_ids(asset_dict)
        return Dataset(**asset_dict)

    def add_tags(self, tags: List[Tag], asset_uid: str) -> Dataset:
        """ Add new asset tags to an existing asset_tags data set
        with the given uid.
        Parameters
        ----------
        tags : List[Tag]

        asset_tags_uid : str
            ID of the existing tag set to update


        Returns
        ----------
        Dataset
            The updated tag set (full asset tag object)
        """

        doc_tags = self._collection_dataset.find_one({'uid': asset_uid})
        if not doc_tags:
            raise DatasetNotFound(f"no asset with id: {asset_uid}")

        if not doc_tags.get('tags'):
            doc_tags['tags'] = []

        for tag in tags:
            doc_tags['tags'].append(tag.dict())

        # Takes a asset tag uid key to find one to change and then passes in
        # new tag array
        self._collection_dataset.update_one(
                {'uid': asset_uid},
                {'$set': {'tags': doc_tags['tags']}})

        # doc_tags = self._collection_asset.find_one({'uid': asset_uid})
        self._clean_mongo_ids(doc_tags)
        return Dataset.parse_obj(doc_tags)

    def find_tag_sources(self, **search_filters) -> Iterator[TagSource]:
        """ Searches database for tags using the search_filters as query terms.

        Parameters
        ----------
        search_filters: str, str
            keyword arguments that are added to underlying query

        Yields
        -------
        Iterator[TagSource]
            [description]
        """
        subqueries = []
        query = {}
        for k, v in search_filters.items():
            subqueries.append({k: v})
        if len(subqueries) > 0:
            query = {"$and": subqueries}
        for tagger in self._collection_tag_sources.find(query):
            self._clean_mongo_ids(tagger)
            yield TagSource.parse_obj(tagger)

    def retrieve_dataset(self, uid) -> Dataset:
        """Find a single asset set with the provided-uid

        Parameters
        ----------
        uid : strrrrrrrrrr
            uid of the tagset to return

        Returns
        -------
        dict
            asset set dictionary corresponding to the uid
        """
        doc_tags = self._collection_dataset.find_one({'uid': uid})
        self._clean_mongo_ids(doc_tags)
        return Dataset(**doc_tags)

    def find_datasets(self, skip=0, limit=10, **search_filters) -> Iterator[Dataset]:
        """Find all TagSets matching search filters

        Parameters
        ----------
        search_filters: str, str
            keyword arguments that are added to underlying query

        Returns
        -------
            single TagSet dict
        """
        subqueries = []
        query = {}
        for k, v in search_filters.items():
            if v is not None:
                subqueries.append({k: v})
        if len(subqueries) > 0:
            query = {"$and": subqueries}
        cursor = self._collection_dataset.find(query)
        # This [skip:limit] will be inefficient the query is massive and you
        # only want 50 of the results.  For a temp fix this works, but
        # shouldn't be the permanent solution.
        for item in cursor[skip:limit]:
            self._clean_mongo_ids(item)
            yield Dataset.parse_obj(item)

    def _create_indexes(self):
        self._collection_tag_sources.create_index([
            ('type', 1),
        ])

        self._collection_tag_sources.create_index([
            ('name', 1),
        ]),

        self._collection_tag_sources.create_index([
            ('uid', 1)
        ], unique=True)

        self._collection_tag_sources.create_index([
            ('model_info.label_index', 1)
        ])

        self._collection_tagging_event.create_index([
            ('tagger_id', 1),
        ])

        self._collection_tagging_event.create_index([
            ('uid', 1)
        ], unique=True)

        self._collection_dataset.create_index([
            ("$**", "text"),
        ]),

        self._collection_dataset.create_index([
            ('tags.name', 1),
        ]),

        self._collection_dataset.create_index([
            ('tags.value', 1),
        ])

        self._collection_dataset.create_index([
            ('tags.confidence', 1),
        ]),

        self._collection_dataset.create_index([
            ('uid', 1)
        ], unique=True)

    @staticmethod
    def _inject_uid(tagging_dict):
        if tagging_dict.get('uid') is None:
            tagging_dict['uid'] = str(uuid.uuid4())


    @staticmethod
    def _clean_mongo_ids(data):
        if '_id' in data:
            # Remove the internal mongo id before schema validation
            del data['_id']


class Context():
    db: MongoClient = None
    tag_svc: TagService = None


context = Context()


class BadDataError(Exception):
    def __init__(self, message, error_list):
        self.message = message
        for error in error_list:
            print(error)
