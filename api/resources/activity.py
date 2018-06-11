from api import *
from datetime import datetime
import uuid


class TagViews(flask_resource):
    def __init__(self, site='common'):
        self.site = site
        self.parser = flask_parser.RequestParser()
        # user cookie 
        self.parser.add_argument('suida', location='cookies', required=True)
        # loggen in user cookie
        self.parser.add_argument('suid')
        self.parser.add_argument('object_type', help='Object type')
        self.parser.add_argument('object_id', type=int, action='append', help='Objects list')
        self.parser.add_argument('percentiles', type=bool, help='Return percentiles for tags')
        self.mongo_activity = mongo_client.statistics.activity
        self.mongo_percentiles = mongo_client.statistics.activity_percentiles
        self.mongo_cookies = mongo_client.statistics.cookies

    def get_uid(self, suida, suid=None):
        uid = None
        is_new = False
        if suid is not None:
            _suid = list(self.mongo_cookies.find({'suid': suid, 'site': self.site}))
            if _suid:
                if suida in [r['suida'] for r in _suid]:
                    return _suid[0]['uid'], is_new
                else:
                    uid = _suid[0]['uid']
                    self.mongo_cookies.insert(
                        {'suida': suida, 'suid': suid, 'uid': uid, 'site': self.site},
                    )
                    return uid, is_new

        _suida = list(self.mongo_cookies.find({'suida': suida, 'site': self.site}))
        if _suida:
            if suid in [r['suid'] for r in _suida]:
                return _suida[0]['uid'], is_new
            else:
                uid = _suida[0]['uid']
                self.mongo_cookies.insert(
                    {'suida': suida, 'suid': suid, 'uid': uid, 'site': self.site},
                )
                return uid, is_new

        if uid is None:
            uid = uuid.uuid4().hex
            is_new = True
            self.mongo_cookies.insert(
                {'suida': suida, 'suid': suid, 'uid': uid, 'site': self.site},
            )
            return uid, is_new
        else:
            return uid, is_new

    def add_data(self, uid, object_type, object_id):
        try:
            for oid in object_id:
                self.mongo_activity.update(
                    {'uid': uid, 'type': object_type, 'id': oid,
                     'method': 'views', 'site': self.site},
                    {'$set': {'time': datetime.now()}, '$inc': {'views': 1}},
                    upsert=True, multi=False
                )
            return None
        except Exception as e:
            return e

    def get_data(self, uid, object_type=None, percentiles=False):
        try:
            if object_type is None:
                d = list(self.mongo_activity.find(
                    {'uid': uid, 'site': self.site,  'is_removed': {'$ne': True}},
                    {'_id': 0, 'id': 1, 'views': 1}
                ).sort('views', -1).limit(100))
            else:
                d = list(self.mongo_activity.find(
                    {'uid': uid, 'type': object_type, 'site': self.site,  'is_removed': {'$ne': True}},
                    {'_id': 0, 'id': 1, 'views': 1}
                ).sort('views', -1).limit(100))
            
            if percentiles:
                for i, obj in enumerate(d):
                    try:
                        d[i]['percentile'] = self.mongo_percentiles.find_one(
                            {'site': self.site, 'id': obj['id'], 'value': {'$lte': obj['views']}},
                            {'_id': 0, 'percentile': 1}
                        )['percentile']
                    except (KeyError, TypeError):
                        d[i]['percentile'] = 0
            return d
        except Exception as e:
            logger.error(e)
            return []

    def remove_object(self, uid, object_type, object_id):
        for oid in object_id:
            self.mongo_activity.update(
                {'suida': uid, 'type': object_type, 'site': self.site, 'id': oid, 'method': 'views'},
                {'$set': {'is_removed': True}},
                True, False
            )

    def get(self):
        try:
            request_args = self.parser.parse_args()
            uid, is_new = self.get_uid(request_args['suida'], request_args['suid'])
            if request_args['object_type'] is not None and request_args['object_id'] is not None:
                add_exc = self.add_data(
                    uid, request_args['object_type'], request_args['object_id']
                )
            else:
                add_exc = None
            if add_exc is None:
                return {
                    'is_new': is_new,
                    'views': self.get_data(uid, request_args['object_type'], percentiles=request_args['percentiles'])
                }
            else:
                flask_abort(500, error_message=str(add_exc))
        except (KeyError, ValueError):
            flask_abort(400)

    def delete(self):
        try:
            request_args = self.parser.parse_args()
            uid, is_new = self.get_uid(request_args['suida'], request_args['suid'])
            self.remove_object(uid, request_args['object_type'], request_args['object_id'])
        except KeyError:
            flask_abort(400)

