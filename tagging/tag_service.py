import uuid


class TagService():
    """The TagService provides access to the tagging database
    as well as an interface into the datbroker which has ingested
    taggable files (both raw and thumbnailed)
    
    Usage looks something like:
    tag_svc = TagService(pymongo_client)
    tag_svc.create_tag_set(tag_set)
    """
    _db = None
    #https://www.mongodb.com/blog/post/building-with-patterns-the-schema-versioning-pattern
    SCHEMA_VERSION = "0.01"

    def __init__(self, client, db_name=None, root_catalog=None):
        """Initialize a TagService entry using the 
        With the provided pymongo.MongoClient instance, the 
        service will create:
        - a database called 'tagging'
        - a collection called 'tag_sets'
        - a collection called 'tag_events'
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
        self._collection_tag_sets = self._db.tag_sets
        self._collection_tag_events = self._db.tagging_events
        self._root_catalog = root_catalog
        self._create_indexes()

    def create_tagging_event(self, tag_event):
        """
        Create a new tag event. The uid from this tag
        event will act like a session for all create TagSets
        in create_tag_set
        Parameters
        ----------
        tagg_event : dict
              tag_event : dict
        {
            "uid": "gc600210432b8f81ad229c12",
            "model_name": "AlexNet"
            "metdata": [
                {"key": "tagger", "value": "ingestor code"}
            ]
        }

        Returns
        ----------
        str
            uid for this event, which can be added for all TagSets
        """
        self._inject_uid(tag_event)
        tag_event['schema_version'] = self.SCHEMA_VERSION
        self._collection_tag_events.insert_one(tag_event)
        return tag_event['uid']

    def get_tagging_event(self, uid):
        tagging_event = self._collection_tag_events.find_one({'uid': uid})
        return tagging_event

    def create_tag_set(self, tag_set, tag_event_uid):
        """Create a new tag set. 
        
        Parameters
        ----------
        tag_set : dict
        
        tags 

        format should be:
        {
        "tags": [
        {
            "key": "scattering_geometry",
            "value": "transmission",
            "confidence": 0.9008, 
        },
        {
            "key": "sample_detector_distance_name",
            "value": "WAXS",
            "confidence": 0.001, 
            "tag_event": "8c600210432b8f81ad229c1f",
        }
        ]
        }
        
      

        Returns
        -------
        str
            generated uid of the new tag set with new tag event
        """
        self._inject_uid(tag_set)
        tag_set['schema_version'] = self.SCHEMA_VERSION
        for tag in tag_set['tags']:
            tag['tag_event'] = tag_event_uid
        self._collection_tag_sets.insert_one(tag_set)
        return tag_set['uid']

    def find_tag_set(self, uid):
        """Find a single TagSet with the provided-uid
        
        Parameters
        ----------
        uid : str
            uid of the tagset to return
        
        Returns
        -------
        dict
            TagSet dictionary of corresponding to the uid
        """
        doc_tags = self._collection_tag_sets.find_one({'uid': uid})
        return doc_tags

    def find_random_tag_sets(self, size):
        """Find random TagSets with no filtering
        
        Parameters
        ----------
        size : int
            number of TagSets to return
        
        Returns
        -------
        cursor
            cursor for the query
        """
        return self._collection_tag_sets.aggregate([{'$sample': {'size': size}}])

    def find_tag_sets_one_filter(self, tag_name, tag_value):
        """Find all TagSets matching tag_name and tag_value
        
        Parameters
        ----------
        tag_name : str
            name of tag to query on
        tag_value : str
            value of tag to query on
        
        Returns
            single TagSet dict
        """
        filter = {'tags.key': tag_name, 'tags.value': tag_value}
        return self._collection_tag_sets.find(filter)


    def find_tag_sets_mongo(self, mongo_filter):
        """Full mongo find 
        While other find methods provide convenience with name/value
        pairs, this is used for situations where the 
        caller needs the full power and complexity of the mongo find features.
        For example:
            {'tags.num_spokes': {'$gt': 3}}
        returns all TagSets with a {'num_spokes': } greter than 3

        Parameters
        ----------
        mongo_filter : dict 
            mongo find input parameter

        Returns
        -------
        cursor
            cursor for the query
        """
        return self._collection_tag_sets.find(mongo_filter)

    def _create_indexes(self):

        self._collection_tag_sets.create_index([
            ('tags.key', 1),
            ('tags.value', 1),
        ])

        self._collection_tag_sets.create_index([
            ('tags.key', 1)
        ])

        self._collection_tag_sets.create_index([
            ('tags.value', 1),
        ])
        
        self._collection_tag_sets.create_index([
            ('uid', 1)
        ], unique=True)

        self._collection_tag_sets.create_index([
            ('tags.tag_event', 1)
        ])

        self._collection_tag_sets.create_index([
            ('tags.confidence', 1)
        ])

        self._collection_tag_events.create_index([
            ('md.key', 1),
            ('md.value', 1)
        ])

        self._collection_tag_events.create_index([
            ('name', 1)
        ])

        self._collection_tag_events.create_index([
            ('uid', 1)
        ], unique=True)

    @staticmethod
    def _inject_uid(tagging_dict):
        if tagging_dict.get('uid') is None:
            tagging_dict['uid'] = str(uuid.uuid4())
