# -*- coding: utf-8 -*-

from common.config import pg_config, ch_config, mg_config
from common.helpers import encode
from time import sleep
import requests
import json

import logging
logger = logging.getLogger(__name__)



class PG(object):
    _db_connection = None
    _db_cursor = None

    def __init__(self, autocommit=True, dict_cursor=False):
        import psycopg2
        from psycopg2.extras import DictCursor, RealDictCursor, execute_values
        from psycopg2.extensions import AsIs
        # db_config = config['postgres'].get(connect_type).copy()
        # db_config['user'] = db_config['user'].replace('ru', connect_user)
        cursor = RealDictCursor if dict_cursor else DictCursor
        self._db_connection = psycopg2.connect(**pg_config)
        self._db_connection.autocommit = autocommit
        self.AsIs = AsIs
        self.execute_values = execute_values
        self._db_cursor = self._db_connection.cursor(cursor_factory=cursor)

    def connection(self):
        return self._db_connection

    def set_autocommit(self, autocommit):
        self._db_connection.autocommit = autocommit

    def query(self, query, params=None):
        return self._db_cursor.execute(query, params)

    def fetchall(self):
        return self._db_cursor.fetchall()

    def fetchone(self):
        return self._db_cursor.fetchone()

    def commit(self):
        return self._db_connection.commit()

    def rollback(self):
        return self._db_connection.commit()

    def print_query(self, query, params=None):
        return self._db_cursor.mogrify(query, params).decode('utf8')

    def executemany(self, query, params=None):
        return self._db_cursor.executemany(query, params)

    def executevalues(self, query, data):
        return self.execute_values(self._db_cursor, query, data, template=None, page_size=100)

    def rowcount(self):
        return self._db_cursor.rowcount

    def listen(self, channel):
        self.query('LISTEN %(channel)s;', {'channel': self.AsIs(channel)})
        while 1:
            self._db_connection.poll()
            while self._db_connection.notifies:
                n = self._db_connection.pop()
                print(n)
            sleep(5)

    def __del__(self):
        self._db_connection.close()


class ClickHouse:
    from common.decorators import retry

    def __init__(self, connect_type='master'):
        self.db_config = ch_config
        self.url = 'http://{host}:{port}'.format(host=self.db_config['host'], port=self.db_config['port'])
        self.params = {'user': self.db_config['user'], 'database': self.db_config['dbname'],
                       'password': self.db_config['password']}

    def _send(self, data, stream=False, **kwargs):
        try:
            data = data.encode('utf-8')
        except AttributeError:
            pass
        params = dict(self.params, **kwargs)
        r = requests.post(self.url, params=params, data=data, stream=stream)
        if r.status_code != 200:
            if 'Code: 62' in r.text and self.get_query_type(data) in ('select', 'show'):
                logger.warning('empty query')
            else:
                raise Exception(r.text)
        return r

    @staticmethod
    def _merge_tree_parser(create_query):
        if 'mergetree' not in create_query.lower():
            return None
        import re
        query_format = re.compile(
            r'(?P<query>[A-Z ]+) (?P<db>[a-z]+)\.(?P<table>[a-z_]+) \((?P<columns>.*?)\) '
            r'ENGINE = (?P<engine>[a-zA-Z]+)\((?P<engine_params>.*?)\)end'
        )
        m = query_format.search(create_query + 'end')
        columns = list()
        for col in m.group('columns').strip().split(',  '):
            prms = col.split(' ')
            column = {'name': prms[0], 'type': prms[1]}
            if len(prms) > 2:
                column[prms[2].lower()] = prms[3]
            columns.append(column)
        engine_prms = m.group('engine_params').split(', ')
        if len(engine_prms) == 3:
            engine_params = {'date': engine_prms[0], 'keys': engine_prms[1].split(', '), 'granularity': engine_prms[2]}
        elif len(engine_prms) == 4:
            engine_params = {'date': engine_prms[0], 'hash': engine_prms[1],
                             'keys': engine_prms[2].split(', '), 'granularity': engine_prms[3]}
        else:
            engine_params = {}
        return {'query': m.group('query'), 'db': m.group('db'), 'table': m.group('table'), 'engine': m.group('engine'),
                'columns': columns, 'engine_params': engine_params}

    @staticmethod
    def get_query_type(q):
        query_type = q.strip().split(' ')[0].lower()
        return query_type

    def query(self, q, format_='CSV', output=None, **kwargs):
        if format_ != 'file':
            if self.get_query_type(q) in ('select', 'show'):
                q += ' FORMAT {fmt}'.format(fmt=format_)
            if output is not None:
                result = self._send(q, True, **kwargs)
                with open(output, 'wb') as f:
                    for chunk in result.iter_content(chunk_size=1048576):
                        if chunk:
                            f.write(chunk)
                return output
            else:
                result = self._send(q, **kwargs)
                if format_ == 'JSON':
                    return result.json()
                else:
                    return result.text
        else:
            try:
                with open(q, 'rb') as f:
                    self._send(f, True, **kwargs)
            except FileNotFoundError:
                logger.warning('File not found')


    # @retry(5, 10)
    def import_file(self, file_name, table=None, columns=None, fmt='CSV'):
        import codecs
        if table is not None:
            cl = '' if columns is None else '({0})'.format(','.join(columns))
            with codecs.open(file_name, mode='r') as csv_file:
                q = 'INSERT INTO {table} {columns} FORMAT {fmt}\n {csv}'.format(
                    table=table, columns=cl, fmt=fmt, csv=csv_file.read()
                )
                return self.query(q)
        else:
            self.query(file_name, format_='file', input_format_allow_errors_ratio=0.005)

    def import_dataframe(self, table, df):
        data = df.to_csv(index=False).split('\n')
        q = "INSERT INTO {table} ({fields}) FORMAT CSV \n".format(table=table, fields=data[0])
        q += '\n'.join(data[1:])
        return self.query(q)

    def optimize(self, table):
        q = 'OPTIMIZE TABLE {table}'.format(table=table)
        return self.query(q)

    def finalize(self, table):
        """Работает только с движками *ingMergeTree, но не самим MergeTree"""
        logger.info('Finalizing table')
        table_split = table.split('.')
        if len(table_split) == 1:
            table_name = table
            schema = self.db_config['dbname']
        else:
            schema = table_split[0]
            table_name = table_split[1]
        full_table = '{0}.{1}'.format(schema, table_name)
        temp_table = '{0}_temp'.format(full_table)
        old_table = '{0}_old'.format(full_table)
        create_query = self.query('SHOW CREATE TABLE {table}'.format(table=table), format_='JSON').get(
            'data', [])[0].get('statement').replace(full_table, temp_table)
        table_params = self._merge_tree_parser(create_query)
        if table_params is not None and table_params['engine'] != 'MergeTree':
            self.query(create_query)
            self.query('INSERT INTO {0} SELECT * FROM {1} FINAL'.format(temp_table, full_table))
            self.query('RENAME TABLE {0} TO {1}'.format(full_table, old_table))
            self.query('RENAME TABLE {0} TO {1}'.format(temp_table, full_table))
            self.query('DROP TABLE {0}'.format(old_table))
        else:
            logger.warning('Bad table engine')

    class FileWriter:
        def __init__(self, fn, t, c):
            self.filename = fn
            self.table = t
            self.columns = c
            self.filename.write('INSERT INTO {t} ({c}) FORMAT TSV\n'.format(t=self.table, c=','.join(self.columns)))

        def write_row(self, d):
            row = []
            for c in self.columns:
                if isinstance(d[c], dict):
                    d[c] = json.dumps(d[c], separators=(',', ':'))
                row.append(encode(d[c]).replace('\\', '\\\\').replace('\\N', 'N'))
            self.filename.write('\t'.join(row) + '\n')


class Mongo:
    def __init__(self, **kwargs):
        from pymongo import MongoClient
        from pymongo.errors import CursorNotFound
        self.mongo = MongoClient(
            **mg_config,
            readPreference='secondaryPreferred',
            **kwargs
        )
        self.CursorNotFound = CursorNotFound


