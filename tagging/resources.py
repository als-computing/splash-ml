from bson.json_util import dumps, loads
from flask import request
from flask_restful import Resource
from tagging.tag_service import TagService


class Event(Resource):
    def __init__(self, tag_svc: TagService):
        super().__init__()
        self._tag_svc = tag_svc

    def get(self, uid):
        results = self._tag_svc.get_tagging_event(uid)
        return dumps(results)

    def put(self, uid):
        data = loads(request.data)
        if data.get('uid') == uid:
            self._tag_svc.create_tagging_event(data)
        else:
            raise KeyError("uid of payload deos not match uid of put")

class Events(Resource):
    def __init__(self, tag_svc: TagService):
        super().__init__()
        self._tag_svc = tag_svc
    
    def post(self):
        data = loads(request.data)
        self._tag_svc.create_tagging_event(data)
        return dumps({'message': 'CREATE SUCCESS', 'uid': str(data['uid'])})
    
   