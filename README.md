PySpark Streaming vs Batch Tutorial (wip)
=========================================

The idea of this tutorial is to show how code can be shared between streaming and batch analysis in pyspark (see the functions in `analysis.py`).

The focus is maintenance of the code in the long term, i.e. you want to update your analysis functions, without affecting both streaming and batch pipelines.

Batch is currenty showing 2 use cases:
1. relaunch hashtag analysis -- think you want to have data on a specific temporal window
2. recompute keywords and relaunch analysis -- think you have an improved algorithm and need to update all historical data

This is a work in progress.

TODO:
- storage (relations, update)
- a consumer, like a web ui?
- refactoring
- better use of cluster


Running the Demo
----------------

Pre-req: a cluster with pyspark working.

In shell 1, feed some tweets:
```
$ nc -l -p 9999 -c "python3 tweets.py"
```

In shell 2, run the streaming app:
```
$ spark-submit app.py
```

When a bit of data is available, in shell 3 run the batch app:
```
$ spark-submit batch.py
```
