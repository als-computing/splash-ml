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

    def __init__(self, client):
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

        """
        self._db = client.tagging
        self._collection_tag_sets = self._db.tag_sets
        self._coolection_tag_events = self._db.tag_events
        self._create_indexes()

    def create_tag_set(self, tag_set):
        """Create a new tag set. 
        
        Parameters
        ----------
        tag_set : dict
            format should be:
            {
        "tags": [
        {
            "key": "scattering_geometry",
            "value": "transmission",
            "confidence": 0.9008, 
            "tag_event": "8c600210432b8f81ad229c1f",
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
            generated uid of the new tag set
        """
        self._inject_uid(tag_set)
        # self._col_doc_tags.insert_one(json_util.loads(json_tag_set))
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
        -------
        single TagSet dict
        """
        filter = {'tags.' + tag_name: tag_value}
        return self._collection_tag_sets.find(filter)

    def find_tag_sets_multi_filter(self, filter_list):
        """Find TagSets using multiple tag name/value pairs to filter on
        
        Parameters
        ----------
        filter_list : list of tuples
            tuple (name, values) to filter on
            e.g. [('color', 'red'), ('frame_material', 'steel')]
        
        Returns
        -------
        cursor
            cursor for the query
        """
        filters = {}
        for tag_pair in filter_list:
            filters['tags.' + tag_pair[0]] = tag_pair[1]
        return self._collection_tag_sets.find(filters)

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
            ('uid', 1)
        ], unique=True)

        self._collection_tag_sets.create_index([
            ('tags.key', 1),
            ('tags.value', 1),
        ])

        self._collection_tag_sets.create_index([
            ('tags.confidence', 1)
        ])
    
    @staticmethod
    def _inject_uid(tagging_dict):
        if tagging_dict.get('uid') is None:
            tagging_dict['uid'] = str(uuid.uuid4())
