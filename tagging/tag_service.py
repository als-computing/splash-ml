import uuid
import jsonschema
from .schemas import schema_tagger, schema_asset_tags, schema_tagging_event


class TagService():
    """The TagService provides access to the tagging database
    as well as an interface into databroker which has ingested
    taggable files (both raw and thumbnailed)
    
    Usage looks something like:
    tag_svc = TagService(pymongo_client)
    tag_svc.create_tagger(tagger)
    """
    _db = None
    # https://www.mongodb.com/blog/post/building-with-patterns-the-schema-versioning-pattern
    SCHEMA_VERSION = "0.01"

    def __init__(self, client, db_name=None, root_catalog=None):
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
        self._collection_tagger_sets = self._db.tagger_sets
        self._collection_tagging_events = self._db.tagging_events
        self._collection_asset_tags = self._db.asset_tags
        self._root_catalog = root_catalog
        self._create_indexes()


    def _clean_mongo_ids(self, data):
        if '_id' in data:
            # remove the mongo id before validating
            del data["_id"]


    def create_tagger(self, tagger):
        """
        Create a new tagger data set. The uid from this tagger will act like a
        session for all create tagging_events in create_tagging_event
        Parameters
        ----------
        tagger : dict
        {
            "uid": "taggerUID",
            "type": "model | human",
            "model_name": "modelName",
            "create_time": #
        }

        Returns
        ----------
        str
            uid for this tagger, which can be added for all Tagging events
        """
        self._inject_uid(tagger)
        validation_errors = self.validate_json(tagger, schema_tagger)
        if len(validation_errors) == 0:
            tagger['schema_version'] = self.SCHEMA_VERSION
            self._collection_tagger_sets.insert_one(tagger)
            self._clean_mongo_ids(tagger)
            return tagger['uid']
        else:
            raise BadDataError("Bad data", validation_errors)
            # TODO: raise error or return the error list like splash?
            #       would have to restructure tests and ingest if latter
            # return {"Bad data, error(s) were encountered": validation_errors}

    def create_tagging_event(self, tagging_event, tagger_uid):
        """
        Create a new tagging_event data set.  The uid for this tag event will label
        the event to create asset tags on 
        Parameters
        ----------
        tagging_event : dict
        {
            "uid": "taggingEventUID",
            "tagger_id": "taggerUID",
            "run_time": #,
            "accuracy": #
        }

        tagger_uid : str
            ID of this event's tagger

        Returns
        ----------
        str
            uid for this event, which can be added to asset_tags
        """
        self._inject_uid(tagging_event)
        tagging_event['tagger_id'] = tagger_uid
        validation_errors = self.validate_json(tagging_event, schema_tagging_event)
        if len(validation_errors) == 0:
            tagging_event['schema_version'] = self.SCHEMA_VERSION
            self._collection_tagging_events.insert_one(tagging_event)
            self._clean_mongo_ids(tagging_event)
            return tagging_event['uid']
        else:
            raise BadDataError("Bad data", validation_errors)
            # return {"Bad data, error(s) were encountered": validation_errors}

    def create_asset_tags(self, asset_tags, tagging_event_uid):
        """
        Create a new asset_tags data set.  The uid for this asset tag set
        distinguishes it from others and can be used to find it later.
        Parameters
        ----------
        asset_tags : dict
        {
            "uid": "assetTagsUID,
            "sample_id": "randomid",
            "tags": [
                {
                    "tag": "attribute",
                    "confidence": #
                    "event_id": "taggingEventUID",
                }
            ]
        }

        tagging_event_uid : str
            ID of this tag set's tagging event

        Returns
        ----------
        str
            uid for this tag set, which can be used to find and add more tags to
            asset tags
        """
        self._inject_uid(asset_tags)
        for tags in asset_tags['tags']:
            tags['event_id'] = tagging_event_uid
        validation_errors = self.validate_json(asset_tags, schema_asset_tags)
        if len(validation_errors) == 0:
            asset_tags['schema_version'] = self.SCHEMA_VERSION
            self._collection_asset_tags.insert_one(asset_tags)
            self._clean_mongo_ids(asset_tags)
            return asset_tags['uid']
        else:
            raise BadDataError("Bad data", validation_errors)
            # return {"Bad data, error(s) were encountered": validation_errors}

    def add_asset_tags(self, tags, asset_tags_uid, tagging_event_uid):
        """
        Add new asset tags to an existing asset_tags data set
        with the given uid.
        Parameters
        ----------
        tags : list
        [
            {
                "tag": "attribute",
                "confidence": #
                "event_id": "taggingEventUID",
            },
            {
                "tag": "attribute2",
                "confidence": #
                "event_id": "taggingEventUID",
            }
        ]

        asset_tags_uid : str
            ID of the existing tag set to update

        tagging_event_uid : str
            ID of the tagging event of the tags in the list

        Returns
        ----------
        dict
            The updated tag set (full asset tag object)
        """
        for tag in tags:
            tag['event_id'] = tagging_event_uid
        schema_tags = schema_asset_tags['properties']['tags']
        validation_errors = self.validate_json(tags, schema_tags)

        if len(validation_errors) == 0:
            # TODO: what if query fails
            doc_tags = self._collection_asset_tags.find_one({'uid': asset_tags_uid})
            doc_tags['tags'].extend(tags)

            # Takes a asset tag uid key to find one to change and then passes in
            # new tag array
            self._collection_asset_tags.update_one(
                    {'uid': asset_tags_uid},
                    {'$set': {'tags': doc_tags['tags']}})

            doc_tags = self._collection_asset_tags.find_one({'uid': asset_tags_uid})
            return doc_tags
        else:
            raise BadDataError("Bad data", validation_errors)
            # return {"Bad data, error(s) were encountered": validation_errors}

    def validate_json(self, data, schema):
        """
        Validate incoming data against a particular schema,
        returning a list of eny errors encountered.

        Parameters
        ----------
        data
            Data to be validated with the passed-in schema before
            being added to the database
        schema : dict
            A JSON schema to use for validating the given data

        Returns
        ----------
        list
            List of tuples with information about any validation errors
        """
        validator = jsonschema.Draft7Validator(schema, format_checker=jsonschema.draft7_format_checker)
        validate_data = data.copy()
        errors = validator.iter_errors(validate_data)
        error_list = [(error.message, str(error.path), error) for error in errors]
        return error_list

    def get_tagger(self, uid):
        tagger = self._collection_tagger_sets.find_one({'uid': uid})
        return tagger

    def find_asset_set(self, uid):
        """Find a single asset set with the provided-uid
        
        Parameters
        ----------
        uid : str
            uid of the tagset to return
        
        Returns
        -------
        dict
            asset set dictionary of corresponding to the uid
        """
        doc_tags = self._collection_asset_tags.find_one({'uid': uid})
        return doc_tags

    def find_random_event_sets(self, size):
        """Find random Tagging events with no filtering
        
        Parameters
        ----------
        size : int
            number of Tagging events to return
        
        Returns
        -------
        cursor
            cursor for the query
        """
        return self._collection_tagging_events.aggregate([{'$sample': {'size': size}}])

    def find_random_asset_sets(self, size):
        """Find random asset tags with no filtering
        
        Parameters
        ----------
        size : int
            number of asset tags to return
        
        Returns
        -------
        cursor
            cursor for the query
        """
        return self._collection_asset_tags.aggregate([{'$sample': {'size': size}}])

    def find_tags_one_filter(self, tag_name, tag_confidence):
        """Find all TagSets matching tag_name and tag_value
        
        Parameters
        ----------
        tag_name : str
            name of tag to query on
        tag_confidence : float
            confidence level of tag to query on
        
        Returns
        -------
            single TagSet dict
        """
        tag_filter = {'tags.tag': tag_name, 'tags.confidence': tag_confidence}
        return self._collection_asset_tags.find(tag_filter)

    def find_asset_sets_mongo(self, mongo_filter):
        """Full mongo find 
        While other find methods provide convenience with name/value
        pairs, this is used for situations where the 
        caller needs the full power and complexity of the mongo find features.
        For example:
            {'tags.confidence': {'$gt': 0.3}}
        returns all TagSets with a {'confidence': } greater than 0.3 or 30%

        Parameters
        ----------
        mongo_filter : dict 
            mongo find input parameter

        Returns
        -------
        cursor
            cursor for the query
        """
        return self._collection_asset_tags.find(mongo_filter)

    def find_events_mongo(self, mongo_filter):
        """Full mongo find 
        While other find methods provide convenience with name/value
        pairs, this is used for situations where the 
        caller needs the full power and complexity of the mongo find features.

        returns all events that fit the mongo filter

        Parameters
        ----------
        mongo_filter : dict 
            mongo find input parameter

        Returns
        -------
        cursor
            cursor for the query
        """
        return self._collection_tagging_events.find(mongo_filter)

    def find_tagger_mongo(self, mongo_filter):
        """Full mongo find 
        While other find methods provide convenience with name/value
        pairs, this is used for situations where the 
        caller needs the full power and complexity of the mongo find features.

        returns all events that fit the mongo filter

        Parameters
        ----------
        mongo_filter : dict 
            mongo find input parameter

        Returns
        -------
        cursor
            cursor for the query
        """
        return self._collection_tagger_sets.find(mongo_filter)

    def _create_indexes(self):
        # Comment - Hey Dylan, correct me if im wrong, but if I make indexes
        # for type, model, and create_time, I dont need to create one for each
        # sepertaly, like the first create index with all 3 should cover even 1
        # variable queries, right?
        self._collection_tagger_sets.create_index([
            ('type', 1),
        ])

        self._collection_tagger_sets.create_index([
            ('model_name', 1),
        ])

        self._collection_tagger_sets.create_index([
            ('accuracy', 1)
        ])

        self._collection_tagger_sets.create_index([
            ('uid', 1)
        ], unique=True)

        self._collection_tagging_events.create_index([
            ('tagger_id', 1),
        ])

        self._collection_tagging_events.create_index([
            ('uid', 1)
        ], unique=True)

        self._collection_asset_tags.create_index([
            ('sample_id', 1),
        ])

        self._collection_asset_tags.create_index([
            ('tags.tag', 1),
        ])

        self._collection_asset_tags.create_index([
            ('tags.confidence', 1),
        ])
        
        self._collection_asset_tags.create_index([
            ('uid', 1)
        ], unique=True)

    @staticmethod
    def _inject_uid(tagging_dict):
        if tagging_dict.get('uid') is None:
            tagging_dict['uid'] = str(uuid.uuid4())


class BadDataError(Exception):
    def __init__(self, message, error_list):
        self.message = message
        for error in error_list:
            print(error)
