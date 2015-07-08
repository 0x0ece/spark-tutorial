import json
import tweepy

class Tweet(): 
    def __init__(self, json_str):
        self.json_str = json_str
        self.status = tweepy.Status.parse(None, json.loads(json_str))
        self._keywords = []
        try:
            self._hashtags = ['#%s' % h['text'] for h in self.status.entities.get('hashtags', [])]
        except:
            self._hashtags = []
    
    def __str__(self):
        return self.status.text

    def hashtags(self):
        return self._hashtags

    def words(self):
        return self.status.text.split(' ')

    def setKeywords(self, keywords):
        self._keywords = keywords

    def keywords(self):
        return self._keywords
