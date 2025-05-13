from urllib.parse import quote
import logging
import requests


class Trakt:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.trakt.tv"
