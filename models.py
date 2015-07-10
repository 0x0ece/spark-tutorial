from sqlalchemy import Column, BigInteger, DateTime, String, Text, orm
from tweepy import Status

from database import Base

import json

class Tweet(Base): 
    __tablename__ = 'tweets'
    id = Column(BigInteger, primary_key=True)
    author_name = Column(String())
    create_time = Column(DateTime)
    raw_json = Column(Text)

    def __init__(self, **kwargs):
        super(Tweet, self).__init__(**kwargs)
        self.init_on_load()

    @orm.reconstructor
    def init_on_load(self):
        self._words = []
        self._keywords = []
        self._hashtags = []

        self.status = None
        if self.raw_json:
            self.status = Status.parse(None, json.loads(self.raw_json))
            
        if self.status:
            self.id = self.status.id
            self.author_name = self.status.author.screen_name
            self.create_time = self.status.created_at
            self._hashtags = ['#%s' % h['text'].lower() for h in self.status.entities.get('hashtags', [])]
            self._words = self.status.text.split(' ')
    
    def __repr__(self):
        return 'Tweet<author=%r, text=%r>' % (self.author_name, self.status.text)

    def hashtags(self):
        return self._hashtags

    def words(self):
        return self._words

    def setKeywords(self, keywords):
        self._keywords = keywords

    def keywords(self):
        return self._keywords
