from flask import Flask, Markup
from flask_restful import Api
from flask_cors import CORS
from flask_caching import Cache
import importlib
from api import is_debug, flask_jsonify

import logging
logger = logging.getLogger('api')


def add_endpoint(lib_name, class_name, endpoint=None, site='common'):
    try:
        imported_module = importlib.import_module('api.resources.{0}'.format(lib_name))
        imported_class = getattr(imported_module, class_name)
    except (AttributeError, ModuleNotFoundError) as e:
        logger.error('Import error: ', e)
        return
    if endpoint is None:
        if lib_name.replace('_', '') == class_name.lower():
            api_endpoint = '/api/{0}/{1}/'.format(site, lib_name)
        else:
            api_endpoint = '/api/{0}/{1}/{2}/'.format(site, lib_name, class_name.lower())
    else:
        api_endpoint = endpoint
    if site == 'common':
        flask_api.add_resource(imported_class, api_endpoint, endpoint=api_endpoint)
    else:
        flask_api.add_resource(
            imported_class, api_endpoint,
            endpoint=api_endpoint, resource_class_kwargs={'site': site}
        )


flask_app = Flask(__name__)
CORS(flask_app)
flask_api = Api(flask_app)
flask_cache = Cache(flask_app, config={'CACHE_TYPE': 'simple'})


@flask_app.route('/api/help/', methods=['GET'])
@flask_cache.cached(timeout=60*60*12)
def info():
    """Prints available endpoints"""
    func_list = {}
    for rule in flask_app.url_map.iter_rules():
        if rule.endpoint != 'static':
            doc = flask_app.view_functions[rule.endpoint].__doc__
            func_list[rule.rule] = doc.split('\n')[0] if doc is not None else doc
    return flask_jsonify(func_list)


add_endpoint('activity', 'TagViews')


if __name__ == '__main__':
    flask_app.run(debug=is_debug, threaded=True)
