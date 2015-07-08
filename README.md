PySpark Streaming vs Batch Tutorial (wip)
=========================================

The idea of this tutorial is to show how code can be shared between streaming and batch analysis in pyspark (see the functions `keywordExtraction`, `analysisHahtagCount`, `analysisKeywordCount`).

The focus is maintenance of the code in the long term, i.e. you want to update your analysis functions, without affecting both streaming and batch pipelines.

This is a work in progress.

TODO:
- batch
- storage
- a consumer, like a web ui?
- refactoring


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
