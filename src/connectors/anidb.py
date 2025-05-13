from urllib.parse import quote
import logging
import requests

class AniDB:
    def __init__(self, api_key):
        self.username = None
        self.password = None
