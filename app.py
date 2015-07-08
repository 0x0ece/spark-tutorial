from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from models import Tweet

CHECKPOINT_DIR='/mnt/hadoop-disk/hadoop/spark/checkpoints'

def storeTweetsRDD(time, rdd):
    def storeTweetsPartition(partition):
        for t in partition:
            # print("Storing tweet: %s" % str(t))
            pass

    if not rdd.isEmpty():
        rdd.foreachPartition(storeTweetsPartition)

def updateTweetsRDD(time, rdd):
    def updateTweetsPartition(partition):
        for t in partition:
            # print("Updating tweet: %s" % (t.keywords()))
            pass

    if not rdd.isEmpty():
        rdd.foreachPartition(updateTweetsPartition)

def keywordExtraction(t):
    # import time
    # time.sleep(5)
    t.setKeywords( t.words() )
    return t

def top(counts):
    def topPartition(partition):
        return sorted(partition, key=lambda p: p[1], reverse=True)[:10]

    return counts.transform(lambda rdd: rdd.mapPartitions(topPartition)
        .sortBy(lambda p: p[1], ascending=False))

def analysisHahtagCount(tweets):
    hashtags = tweets.flatMap(lambda t: t.hashtags())
    pairs = hashtags.map(lambda h: (h, 1))
    hashtagCounts = pairs.reduceByKey(lambda x, y: x + y)
    return top(hashtagCounts)

def analysisKeywordCount(tweets):
    keywords = tweets.flatMap(lambda t: t.keywords())
    pairs = keywords.map(lambda kw: (kw, 1))
    keywordCounts = pairs.reduceByKey(lambda x, y: x + y)
    return top(keywordCounts)

def createStreamingContext():

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[8]", "GlutenTweet")
    ssc = StreamingContext(sc, 2)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    raw = ssc.socketTextStream("localhost", 9999)

    # Convert into models
    tweets = raw.map(lambda r: Tweet(r))

    # Store models
    tweets.foreachRDD(storeTweetsRDD)

    # Sliding window analysis
    window = tweets.window(10*60, 30)
    hashtagCounts = analysisHahtagCount(window)
    hashtagCounts.pprint()

    # Keyword extraction - note tweets is immutable
    tweetsKeyword = tweets.map(lambda t: keywordExtraction(t))

    # Update models
    tweetsKeyword.foreachRDD(updateTweetsRDD)

    # Sliding window analysis
    window2 = tweetsKeyword.window(10*60, 30)
    keywordCounts = analysisKeywordCount(window2)
    keywordCounts.pprint()

    ssc.checkpoint(CHECKPOINT_DIR)
    return ssc

ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, createStreamingContext)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
