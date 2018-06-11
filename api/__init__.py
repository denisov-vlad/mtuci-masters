from common.db import Mongo, PG
from flask_restful import Resource as flask_resource, abort as flask_abort, reqparse as flask_parser
from flask import make_response as flask_response, jsonify as flask_jsonify
import json

import logging
logger = logging.getLogger(__name__)

pg = PG() 
mongo_client = Mongo(serverSelectionTimeoutMS=1500).mongo
to_json = json.loads

is_debug = True
