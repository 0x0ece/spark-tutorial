from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from models import Tweet
from analysis import keywordExtraction, analysisHahtagCount, analysisKeywordCount
# from database import engine

CHECKPOINT_DIR='/mnt/hadoop-disk/hadoop/spark/checkpoints'
PYFILES = ['app.py', 'database.py', 'models.py']
MASTER = '10.240.169.72'

def storeTweetsRDD(time, rdd):
    def storeTweetsPartition(partition):
        # TODO better db sessions
        engine = create_engine('mysql://root@%s/db' % MASTER)
        db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

        for t in partition:
            db_session.add(t)

        db_session.commit()

    if not rdd.isEmpty():
        rdd.foreachPartition(storeTweetsPartition)

def updateTweetsRDD(time, rdd):
    def updateTweetsPartition(partition):
        for t in partition:
            # print("Updating tweet: %s" % (t.keywords()))
            pass

    if not rdd.isEmpty():
        rdd.foreachPartition(updateTweetsPartition)

def createStreamingContext():

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    # sc = SparkContext("spark://%s:7077" % MASTER, "GlutenTweet", pyFiles=PYFILES)
    sc = SparkContext("local[4]", "GlutenTweet", pyFiles=PYFILES)
    ssc = StreamingContext(sc, 2)

    # Create a DStream of raw data
    raw = ssc.socketTextStream(MASTER, 9999)

    # Convert into models
    tweets = raw.map(lambda r: Tweet(raw_json=r))

    # Store models
    tweets.foreachRDD(storeTweetsRDD)

    # Sliding window analysis
    window = tweets.window(20*60, 30)
    hashtagCounts = analysisHahtagCount(window)
    streamTop(hashtagCounts).pprint()

    # Keyword extraction - note tweets is immutable
    tweetsKeyword = tweets.map(lambda t: keywordExtraction(t))

    # Update models
    tweetsKeyword.foreachRDD(updateTweetsRDD)

    # Sliding window analysis
    window2 = tweetsKeyword.window(20*60, 30)
    keywordCounts = analysisKeywordCount(window2)
    streamTop(keywordCounts).pprint()

    ssc.checkpoint(CHECKPOINT_DIR)
    return ssc

ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, createStreamingContext)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
