from urllib.parse import quote
import logging
import requests


class IMDB:
    def __init__(self, api_key):
        self.api_key = api_key
