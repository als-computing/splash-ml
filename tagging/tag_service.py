import uuid


class TagService():
    """The TagService provides access to the tagging database
    as well as an interface into the datbroker which has ingested
    taggable files (both raw and thumbnailed)
    
    Usage looks something like:
    tag_svc = TagService(pymongo_client)
    tag_svc.create_tagger(tagger)
    """
    _db = None
    #https://www.mongodb.com/blog/post/building-with-patterns-the-schema-versioning-pattern
    SCHEMA_VERSION = "0.01"

    def __init__(self, client, db_name=None, root_catalog=None):
        """Initialize a TagService entry using the 
        With the provided pymongo.MongoClient instance, the 
        service will create:
        - a database called 'tagging'
        - a collection called 'tagger'
        - a collection called 'tagging_event'
        - a collection called 'asset_tags'
        - relevent indexes

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
            uid for this event, which can be added for all Tagging events
        """
        self._inject_uid(tagger)
        tagger['schema_version'] = self.SCHEMA_VERSION
        self._collection_tagger_sets.insert_one(tagger)
        return tagger['uid']

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

        Returns
        ----------
        str
            uid for this event, which can be added to asset_tags
        """
        self._inject_uid(tagging_event)
        tagging_event['shema_version'] = self.SCHEMA_VERSION
        tagging_event['tagger_id'] = tagger_uid
        self._collection_tagging_events.insert_one(tagging_event)
        return tagging_event['uid']
        

    def create_asset_tags(self, asset_tags, tagging_event_uid):
        """
        Create a new asset_tags data set.  The uid for this asset event
        distiguishes it from others and can be used to find it later.
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

        Returns
        ----------
        str
            uid for this event, which can be used to find and add more tags to
            asset tags
        """
        self._inject_uid(asset_tags)
        asset_tags['schema_version'] = self.SCHEMA_VERSION
        for tags in asset_tags['tags']:
            tags['event_id'] = tagging_event_uid
        self._collection_asset_tags.insert_one(asset_tags)
        return asset_tags['uid']
            
    def add_asset_tags(self, tags, uid):
        doc_tags = self._collection_asset_tags.find_one({'uid': uid})
        
        doc_tags['tags'].extend(tags)
        # Takes a asset tag uid key to find one to change and then passes in
        # new tag array
        self._collection_asset_tags.update_one(
                {'uid': uid},    
                {'$set':{'tags':doc_tags['tags']}})

        doc_tags = self._collection_asset_tags.find_one({'uid': uid})
        
        return doc_tags 


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
        tag_value : float
            value of tag to query on
        
        Returns
            single TagSet dict
        """
        filter = {'tags.tag': tag_name, 'tags.confidence': tag_confidence}
        return self._collection_asset_tags.find(filter)


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
