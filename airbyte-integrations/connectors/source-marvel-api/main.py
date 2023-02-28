#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_marvel_api import SourceMarvelApi

if __name__ == "__main__":
    source = SourceMarvelApi()
    launch(source, sys.argv[1:])
