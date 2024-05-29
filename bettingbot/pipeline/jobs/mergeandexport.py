#!/usr/bin/env python
# coding: utf-8

# In[1]:


from  pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from pyspark.sql.types import *
from google.cloud import storage
import sys
sys.path.insert(0, '/home/santossorianojorge/bettingsbot/')
spark = SparkSession.builder .appName('app_name') .master('local[*]') .config('spark.sql.execution.arrow.pyspark.enabled', True) .config('spark.sql.session.timeZone', 'UTC') .config('spark.driver.memory','6G') .config('spark.ui.showConsoleProgress', True) .config('spark.sql.repl.eagerEval.enabled', True) .getOrCreate()


# In[2]:


from enum import Enum
from typing import Union, Iterable

class StrEnum(str, Enum):
    @classmethod
    def to_list(cls: Union[Enum, Iterable]):
        return [element.value for element in cls]

class UrlFactory(StrEnum):
    BWIN = 'https://sports.bwin.es/es/sports/eventos/{id}'
    UNIBET = 'https://es.unibet.com/betting/sports/event/{id}'


# In[3]:


storage_client = storage.Client()
bucket = storage_client.get_bucket('bucketbettingbot')
blob = bucket.blob(f'pipeline/final')

# This deletes the previous blobs created by past runs
# TODO: Archive instead of delete blobs
bucket.delete_blobs([x for x in bucket.list_blobs(prefix=f'pipeline/final/')])


# In[4]:


spark.catalog.clearCache()
df = spark.read.json('gs://bucketbettingbot/pipeline/processed/*')


# In[5]:


def arbitrage(odds):
    max1 = max(odds[0])
    max2 = max(odds[1])
    if max1 > max2:
        _min = min(odds[1])
    else:
        max1 = max2
        _min = min(odds[0])
    return ((((1/max1)+(1/_min))*100)-100)*-1

def buildUrl(_id, site):
    if site == 'bwin':
        return UrlFactory.BWIN.value.format(id=_id)
    elif site == 'unibet':
        return UrlFactory.UNIBET.value.format(id=_id)

def notSameSource(source):
    return source[0] == source[1]

adf=(df.filter(F.col('criteria').isNotNull())
 .withColumn('url', F.udf(buildUrl, StringType())(F.col('matchId'), F.col('source')))
 .groupBy('participant1', 'participant2', 'line', 'criteria')
 .agg(F.collect_list('odds').alias('grouped_odds'), F.collect_list('source').alias('sources'), F.collect_list('matchId').alias('matchIds'), F.collect_list('url').alias('urls'))
 .filter(F.udf(notSameSource, BooleanType())(F.col('sources')) == False)
 .filter(F.size('grouped_odds') == 2)
 .withColumn('arbitrage', F.udf(arbitrage, DoubleType())(F.col('grouped_odds')))
 .withColumn('sport', F.lit('Football'))
 .sort(F.col('arbitrage').desc()))


# In[6]:


adf.write.mode('overwrite').format("json").save(f"gs://bucketbettingbot/pipeline/final/")


# In[7]:


raw_blob = bucket.delete_blobs([x for x in bucket.list_blobs(prefix=f'pipeline/processed/')])


# In[ ]:




