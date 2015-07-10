def keywordExtraction(t):
    # import time
    # time.sleep(5)
    t.setKeywords( t.words() )
    return t

def analysisHahtagCount(tweets):
    hashtags = tweets.flatMap(lambda t: t.hashtags())
    pairs = hashtags.map(lambda h: (h, 1))
    hashtagCounts = pairs.reduceByKey(lambda x, y: x + y)
    return hashtagCounts

def analysisKeywordCount(tweets):
    keywords = tweets.flatMap(lambda t: t.keywords())
    pairs = keywords.map(lambda kw: (kw, 1))
    keywordCounts = pairs.reduceByKey(lambda x, y: x + y)
    return keywordCounts
