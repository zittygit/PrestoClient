# -*- coding: utf-8 -*-

"""
presto client for GiftPack framework
:author: 2019.10.26 by ziyezhang
~~~~~~~~~~~~
This module contains the create query and get query results Requests to Presto API
"""
import requests


class PrestoPack:
    def __init__(self, host, port, schema, catalog, user):
        self._host = host
        self._port = port
        self._schema = schema
        self._catalog = catalog
        self._user = user
        self._URL_STATEMENT_PATH = '/v1/statement'
        self._is_finished = False

    @property
    def __http_headers(self):
        header = dict()
        header['X-Presto-Catalog'] = self._catalog
        header['X-Presto-Schema'] = self._schema
        header['X-Presto-Source'] = 'GiftPacks'  # client name
        header['X-Presto-User'] = self._user
        return header

    @property
    def __get_url(self):
        return "http://{host}:{port}{path}".format(
            host=self._host,
            port=self._port,
            path=self._URL_STATEMENT_PATH
        )

    def create_query(self, sql):
        http_response = requests.post(
            url=self.get_url,
            data=sql.encode('utf-8'),
            headers=self.http_headers
        )
        if not http_response.ok:
            self.raise_response_error(http_response)
        http_response.encoding = 'utf-8'
        response = http_response.json()
        return dict(query_id=response.get('id'), next_uri=response.get('nextUri'))

    def get_query_result(self, next_uri):
        uri = next_uri
        while not self._is_finished:
            http_response = requests.get(
                url=uri,
                headers=self.http_headers
            )
            if http_response.status_code == 200:
                http_response.encoding = 'utf-8'
                response = http_response.json()
                uri = response.get('nextUri')
                data = response.get('data')
                if data is not None:
                    yield data
                if uri is None:
                    self._is_finished = True
                    break
            elif http_response.status_code == 503:
                continue
            else:
                break
