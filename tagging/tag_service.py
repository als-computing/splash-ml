import uuid
from typing import Iterator, List
from operator import attrgetter
from pydantic import parse_obj_as

from pymongo.mongo_client import MongoClient

from .model import (
    Dataset,
    PersistedDataset,
    Tag,
    PersistedTag,
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
        self._collection_tagging_event.insert_one(event_dict)
        self._clean_mongo_ids(event_dict)
        return TaggingEvent(**event_dict)

    def retrieve_tagging_event(self, uid: str) -> TaggingEvent:
        t_e_dict = self._collection_tagging_event.find_one({'uid': uid})
        self._clean_mongo_ids(t_e_dict)
        return TaggingEvent.parse_obj(t_e_dict)

    def create_dataset(self, dataset: Dataset) -> PersistedDataset:
        """ Create a new dataset.  The uid for this dataset distinguishes
        it from others and can be used to find it later.

        Parameters
        ----------
        dataset : NewDataset

        Returns
        ----------
        Dataset
            Dataset object, with uid in it
        """
        dataset = parse_obj_as(PersistedDataset, dataset.dict())
        dataset_dict = dataset.dict()
        self._collection_dataset.insert_one(dataset_dict)
        self._clean_mongo_ids(dataset_dict)
        return dataset

    def add_tags(self, tags: List[Tag], dataset_uid: str) -> List[str]:
        """ Add new set of tags to an existing data set with the
        given uid.
        Parameters
        ----------
        tags : List[PersistedTag]

        dataset_uid : str
            ID of the existing dataset set to update

        Returns
        ----------
        added_tags_uid : List[str]
            List of the UIDs of the added tags
        """
        added_tags_uid = []
        # If no tags were sent, returns empty list
        if tags is None:
            return added_tags_uid

        dataset = self._collection_dataset.find_one({'uid': dataset_uid})
        if not dataset:
            raise DatasetNotFound(f"no dataset with id: {dataset_uid}")

        if dataset['tags'] is None:
            self._collection_dataset.update_one(
                {'uid': dataset_uid},
                {'$set': {'tags': []}})

        persisted_tags = parse_obj_as(List[PersistedTag], tags)
        added_tags_uid = list(map(attrgetter('uid'), persisted_tags))
        persisted_tags_dict = [persisted_tag.dict() for persisted_tag in persisted_tags]

        # Appends tags (dict) in list
        self._collection_dataset.update_one(
            {'uid': dataset_uid},
            {'$push': {'tags': {'$each': persisted_tags_dict}}}
        )

        return added_tags_uid

    def delete_tags(self, list_tags_uid: List[str], dataset_uid: str) -> List[str]:
        """ Add new dataset tags to an existing dataset with the
            given uid.
            Parameters
            ----------

            list_tags_uid : List[str]
                List of tags UIDs to delete

            dataset_uid: str
                Dataset UID

            Returns
            ----------
            removed_tags_uid : List[str]
                List of removed tags UIDs
                If tag UID did not exist, returns -1
            """
        # user sent no tags to delete
        if list_tags_uid is None:
            return []

        dataset = self._collection_dataset.find_one({'uid': dataset_uid})
        if not dataset:
            raise DatasetNotFound(f"no dataset with id: {dataset_uid}")

        # if the there are no tags in the dataset, returns list of -1
        if dataset['tags'] is None or dataset['tags']==[]:
            return ['-1']*len(list_tags_uid)

        removed_tags_uid = list_tags_uid
        result = self._collection_dataset.update_many({'uid': dataset_uid},
                                             {"$pull": {'tags':
                                                            {'uid': {'$in': list_tags_uid}}
                                                        }})
        # if the number of deleted elements does not match the number of tags,
        # finds the tag UIDs that were not deleted
        if result.modified_count is not len(list_tags_uid):
            current_tags_uids = [current_tag['uid'] for current_tag in dataset['tags']]
            for i, tag_uid in enumerate(removed_tags_uid):
                if tag_uid not in current_tags_uids:
                    removed_tags_uid[i] = -1
        return removed_tags_uid

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

    def retrieve_dataset(self, uid) -> PersistedDataset:
        """Find a single dataset with the provided-uid

        Parameters
        ----------
        uid : str
            uid of the dataset to return

        Returns
        -------
        dict
            dataset set dictionary corresponding to the uid
        """
        doc_tags = self._collection_dataset.find_one({'uid': uid})
        self._clean_mongo_ids(doc_tags)
        return PersistedDataset(**doc_tags)

    def find_datasets(
        self, 
        uri: str=None,
        tags: List[str]=None,
        offset=0, 
        limit=10, 
        ) -> Iterator[PersistedDataset]:
        # **search_filters) -> Iterator[Dataset]:
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
        if tags:
            subqueries.append(
                {"tags.name": {"$in": tags}})

        if uri:
            subqueries.append(
                {"uri": uri}
            )

        if len(subqueries) > 0:
            query = {"$and": subqueries}
        cursor = self._collection_dataset.find(query).skip(offset).limit(limit)
        for item in cursor:
            self._clean_mongo_ids(item)
            yield PersistedDataset.parse_obj(item)

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

        #self._collection_dataset.create_index([
        #    ('tags.uid', 1),
        #], unique=True)

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
