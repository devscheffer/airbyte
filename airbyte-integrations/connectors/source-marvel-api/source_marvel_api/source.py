#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import os

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth
from datetime import datetime as dt
import hashlib


def hash_params(pub_key, priv_key):
    """Marvel API requires server side API calls to include
    md5 hash of timestamp + public key + private key"""
    timestamp = dt.now().strftime("%Y-%m-%d%H:%M:%S")

    hash_md5 = hashlib.md5()
    hash_md5.update(f"{timestamp}{priv_key}{pub_key}".encode("utf-8"))
    hashed_params = hash_md5.hexdigest()

    return hashed_params


class MarvelApiStream(HttpStream, ABC):
    url_base = "https://gateway.marvel.com"

    def __init__(self, pub_key, priv_key):
        self.pub_key = pub_key
        self.priv_key = priv_key

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        return {"offset": data["offset"] + data["limit"]}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "apikey": self.pub_key,
            "hash": hash_params(self.pub_key, self.priv_key),
            "limit": 1,
        }
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json


class Comics(MarvelApiStream):
    primary_key = None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/v1/public/comics"


# Source
class SourceMarvelApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            params = {
                "apikey": config["pub_key"],
                "hash": hash_params(config["pub_key"], config["priv_key"]),
                "limit": 1,
            }

            res = requests.get("https://gateway.marvel.com/v1/public/comics", params=params)
            if res.status_code == 200:
                return True, None
            else:
                False, res.status_code
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            Comics(
                config["pub_key"],
                config["priv_key"],
            ),
        ]
