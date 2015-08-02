from pyspark import SparkContext

from models import Tweet
from database import db_session
from analysis import keywordExtraction, analysisHahtagCount, analysisKeywordCount
from config import *

PYFILES = ['batch.py'] + PYFILES

# Create a local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("spark://%s:7077" % MASTER, "GlutenTweetBatch", pyFiles=PYFILES)
sc = SparkContext("spark://%s:7077" % 'hadoop-m-unoa', appName="GlutenTweetBatch", pyFiles=PYFILES)

dbTweets = db_session.query(Tweet).all()
tweets = sc.parallelize(dbTweets)

# Hashtag analysis
hashtagCounts = analysisHahtagCount(tweets)
print(hashtagCounts.top(10, key=lambda p: p[1]))

# Keyword extraction - note tweets is immutable
tweetsKeyword = tweets.map(lambda t: keywordExtraction(t))

# Update models
# tweetsKeyword.foreachRDD(updateTweetsRDD)

# Keyword analysis
keywordCounts = analysisKeywordCount(tweetsKeyword)
print(keywordCounts.top(10, key=lambda p: p[1]))
