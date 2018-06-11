from common.helpers import path_joiner, path_basename, encode
from common.config import working_path, logs_path
from common.logparser import LogTail
from common.db import ClickHouse
from urllib.parse import urlparse, parse_qs
import rapidjson
import codecs
import re
import fasteners
import httpagentparser
from datetime import datetime
import dateutil.parser

import logging
logger = logging.getLogger('parse_nginx_logs')

clickhouse = ClickHouse()
lock_name = path_joiner(working_path, path_basename(__file__) + '.lock')


class Clickstream:
    def __init__(self):
        self.custom_keys = (
            'page_type', 'page_id', 'page_section', 'page_tags', 'event_name',
            'event_value', 'event_category', 'event_label', 'mvt.name', 'mvt.value'
        )
        self.null_chars = {
            'common': '\\N', 'string': '', 'integer': 0, 'array': [],
            'date': '0000-00-00', 'datetime': '0000-00-00 00:00:00'
        }
        self.time_str = '%Y-%m-%d %H:%M:%S'
        self.date_str = '%Y-%m-%d'
        # определяются на уровне JS piwik
        self.columns = [
            'event_time', 'site', 'is_mobile', 'url', 'action_name', 'pageview_id',
            'ping', 'generation_speed', 'piwik_id', 'suid', 'suida',
            'first_visit_time', 'last_visit_time', 'visit_count', 'new_visitor',
            'user_ip', 'user_country', 'user_city', 'user_device_brand', 'user_device_model',
            'user_os_name', 'user_os_version', 'user_browser_name', 'user_browser_version',
            'user_browser_resolution', 'user_is_mobile', 'user_is_pc', 'user_is_tablet',
            'user_is_touch', 'user_is_bot', 'source', 'referrer', 'referrer_time',
            'page_type', 'page_id', 'page_section', 'page_tags', 'link',
            'event_category', 'event_name', 'event_value', 'event_label',
            'mvt.name', 'mvt.value'
        ]

    @staticmethod
    def convert_qs(query_string):
        return dict((k, v if len(v) > 1 else v[0]) for k, v in query_string.items())

    @staticmethod
    def str2none(s):
        """
        Converts javascript's null variables to python's None
        :param s: any string
        :return: None if string is javascript's null else string
        """
        return None if s == '' or s == 'null' or s == 'undefined' or s is None else str(s).strip()

    @staticmethod
    def var2int(s, return_number=False):
        """
        Safe (with exception) convert variable to integer
        :param s: probably integer
        :param return_number: option to return 0 if ValueError raises
        :return:
        """
        try:
            return int(s)
        except ValueError:
            return 0 if return_number else None

    @staticmethod
    def var2bool(v):
        """
        Converts variable to boolean
        :param v: any variable
        :return: boolean value if it can be converted else variable
        """
        if isinstance(v, str):
            if v.lower() in ('yes', 'true', 't', 'on',):
                return 1
            elif v.lower() in ('no', 'false', 'f', 'off',):
                return 0
            else:
                return v
        elif isinstance(v, bool):
            return 1 if v else 0
        else:
            return v

    @staticmethod
    def get_site(h):
        """
        Defines site using host name
        :param h: host name string
        :return: (site short name, site is mobile flag)
        """

        return 'site_name', 1

    @staticmethod
    def user_is_mobile(os_name):
        if os_name in ('Linux', 'ChromeOS', 'Mac OS', 'PlayStation', 'Windows', None):
            return False
        else:
            return True

    @staticmethod
    def encode_qs(url):
        """
        Encodes query string in url to prevent unquoted import
        :param url: string
        :return: encoded url
        """
        url = encode(url)
        if url is not None and '?' in url:
            url_parts = url.split('?', 1)
            encoded_qs = re.sub('_+', '_', encode(url_parts[1]))
            string_length = len(encoded_qs)
            if string_length > 0:
                return url_parts[0] + '?' + encoded_qs
            else:
                return url_parts[0]
        else:
            return url

    def convert_row(self, r):
        """
        Converts access.log parsed row for clickhouse's clickstream table format
        :param r: dictionary after apache_log_parser
        :return: dictionary for csvwriter
        """
        try:
            parsed_url = urlparse(r['query_dict'].get('url', '')).netloc
        except TypeError:
            parsed_url = ''
        result = dict()
        result['event_time'] = dateutil.parser.parse(r['time']).strftime(self.time_str)
        result['site'], result['is_mobile'] = self.get_site(parsed_url)
        result['piwik_id'] = r['query_dict'].get('_id')
        result['source'] = self.encode_qs(r['query_dict'].get('_ref'))
        result['url'] = self.encode_qs(r['query_dict'].get('url'))
        result['user_country'] = r.get('country')
        result['user_city'] = r.get('city')

        result['generation_speed'] = abs(
            self.var2int(r['query_dict'].get('gt_ms', 0), True)
        )
        result['first_visit_time'] = r['query_dict'].get('_idts')
        result['last_visit_time'] = r['query_dict'].get('_viewts')
        result['visit_count'] = self.var2int(r['query_dict'].get('_idvc', 0), True)
        result['new_visitor'] = self.var2bool(
            self.var2int(r['query_dict'].get('_idn', 1), True)
        )
        result['referrer'] = self.encode_qs(r['query_dict'].get('urlref'))
        result['referrer_time'] = r['query_dict'].get('_refts')
        result['user_ip'] = r.get('ip', '')
        user_agent = httpagentparser.detect(r['user_agent'])
        result['user_browser_name'] = self.str2none(user_agent.get('browser', {}).get('name'))
        result['user_browser_version'] = self.str2none(user_agent.get('browser', {}).get('version'))
        result['user_browser_resolution'] = r['query_dict'].get('res')
        result['user_os_name'] = self.str2none(user_agent['platform']['name'])
        result['user_os_version'] = self.str2none(user_agent['platform']['version'])
        is_mobile = self.user_is_mobile(result['user_os_name'])
        result['user_is_mobile'] = self.var2bool(is_mobile)
        result['user_is_pc'] = self.var2bool(not is_mobile)
        result['user_is_tablet'] = 0
        result['user_is_touch'] = 0
        result['user_is_bot'] = self.var2bool(user_agent.get('bot', False))
        result['user_device_brand'] = None
        result['user_device_model'] = None
        result['pageview_id'] = r['query_dict'].get('pv_id')
        # defined in GTM
        result['suid'] = r['query_dict'].get('dimension1')
        result['suida'] = r['query_dict'].get('dimension2')
        # custom page-level variables
        cvar = r['query_dict'].get('cvar', '{}')
        try:
            custom_vars = rapidjson.loads(cvar)
            for k, v in custom_vars.values():
                if k.startswith('mvt'):
                    try:
                        result[k] = rapidjson.loads(v)
                        result[k] = [str(itm) for itm in result[k]]
                    except ValueError:
                        result[k] = self.null_chars['array']
                else:
                    result[k] = self.str2none(v)
        except ValueError:
            pass
        try:
            result['page_id'] = int(
                result['page_id']
            ) if result['page_id'] != '0' and result['page_id'] is not None else None
        except (ValueError, KeyError):
            result['page_id'] = None
        try:
            if result['page_tags'] is None:
                result['page_tags'] = []
            else:
                result['page_tags'] = result['page_tags'].split(',')
        except KeyError:
            result['page_tags'] = []
        new_tags = []
        for tag in result['page_tags']:
            try:
                t = int(tag)
                if t != 0:
                    new_tags.append(t)
            except ValueError:
                pass
        result['page_tags'] = new_tags if len(new_tags) > 0 else self.null_chars['array']
        result['action_name'] = r['query_dict'].get('action_name')
        # sends every 15 seconds when user is on page
        result['ping'] = self.var2int(r['query_dict'].get('ping', 0), True)
        # external link tracker
        if 'link' in r['query_dict']:
            result['link'] = r['query_dict']['link']
        elif 'download' in r['query_dict']:
            result['link'] = r['query_dict']['download']
        else:
            result['link'] = None
        result['event_category'] = r['query_dict'].get('e_c')
        result['event_name'] = r['query_dict'].get('e_n')
        result['event_value'] = r['query_dict'].get('e_v')
        if result['event_category'] == 'auto-click':
            result['pageview_id'] = result['event_value']
            result['event_value'] = 'click'
        result['event_label'] = r['query_dict'].get('e_a')
        # tracks showed content with special class
        if 'c_n' in r['query_dict'] or 'c_p' in r['query_dict']:
            result['event_category'] = 'auto-view'
            result['event_label'] = r['query_dict'].get('c_n')
            result['event_name'] = r['query_dict'].get('c_p')
            result['event_value'] = 'view'
        # transforms all time variables to proper type
        for k in ('first_visit_time', 'last_visit_time', 'referrer_time'):
            try:
                tm = int(result[k]) if result[k] is not None else 0
                result[k] = datetime.fromtimestamp(
                    tm
                ).strftime(self.time_str) if tm > 0 else None
            except ValueError:
                result[k] = None
        # defines all unused variables as None
        for k in self.custom_keys:
            if k not in result:
                if k.startswith('mvt'):
                    result[k] = self.null_chars['array']
                else:
                    result[k] = None
        # transforms all none-like variables to None
        for k, v in result.items():
            result[k] = self.null_chars['common'] if v in ('undefined', 'null') or v is None else result[k]
        return result

    @fasteners.interprocess_locked(lock_name)
    def main(self):
        clickstream_file = path_joiner(working_path, 'clickstream3.csv')
        log_path = path_joiner(logs_path, 'piwik_access.log')

        with codecs.open(clickstream_file, mode='w') as cl:
            log = LogTail(log_path)
            logger.info('Start parsing')
            cr = clickhouse.FileWriter(cl, 'clickstream_table_name', self.columns)

            for line in log:
                try:
                    data = rapidjson.loads(line)
                    method, url, http = data['request'].split(' ')
                except:
                    logger.warning(line)
                    continue

                parsed_url = urlparse(url)
                if parsed_url.path != '/piwik':
                    continue
                if method == 'GET':
                    qs = parse_qs(parsed_url.query)
                    data['query_dict'] = self.convert_qs(qs)
                    res = self.convert_row(data)
                    try:
                        cr.write_row(res)
                    except Exception as e:
                        logger.warning(e)
                elif method == 'POST':
                    post_data = bytes(data['body'], 'utf-8').decode('unicode_escape')

                    try:
                        body = rapidjson.loads(post_data)
                        if 'requests' in body:
                            key = 'request'
                            body[key] = body.pop('requests')
                        else:
                            key = 'impressions'
                            try:
                                qs = parse_qs(body['request'])
                            except TypeError:
                                continue
                            d = self.convert_qs(qs)

                        for item in body[key]:
                            if key == 'impressions':
                                data['query_dict'] = dict(d, **item)
                            else:
                                qs = parse_qs(item[1:])
                                data['query_dict'] = self.convert_qs(qs)
                            try:
                                res = self.convert_row(data)
                                try:
                                    cr.write_row(res)
                                except Exception as e:
                                    logger.warning(e)
                            except ValueError as e:
                                logger.warning(e)
                    except (ValueError, KeyError, TypeError):
                        qs = parse_qs(post_data)
                        data['query_dict'] = self.convert_qs(qs)
                        try:
                            res = self.convert_row(data)
                            try:
                                cr.write_row(res)
                            except ValueError as e:
                                logger.warning(e)
                        except Exception as e:
                            logger.warning(e)

        logger.info('Update tables')
        clickhouse.import_file(clickstream_file)


if __name__ == '__main__':
    Clickstream().main()
