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
        self._inject_uid(tag_set)
        # self._col_doc_tags.insert_one(json_util.loads(json_tag_set))
        self._collection_tag_sets.insert_one(tag_set)
        return tag_set['uid']

    def get_tag_set(self, uid):
        doc_tags = self._collection_tag_sets.find_one({'uid': uid})
        return doc_tags

    def get_random_tag_sets(self, size):
        return self._collection_tag_sets.aggregate([{'$sample': {'size': size}}])

    def get_tag_sets_one_filter(self, tag_name, tag_value):
        filter = {'tags.' + tag_name: tag_value}
        return self._collection_tag_sets.find(filter)

    def get_tag_sets_multi_filter(self, filter_dict):
        filters = {}
        for tag_pair in filter_dict:
            filters['tags.' + tag_pair[0]] = tag_pair[1]
        return self._collection_tag_sets.find(filters)

    def get_tag_sets_mongo(self, mongo_filter):
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
