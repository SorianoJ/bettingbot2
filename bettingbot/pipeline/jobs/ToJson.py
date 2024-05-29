#!/usr/bin/env python
# coding: utf-8

# In[1]:


from  pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from pyspark.sql.types import *
from itertools import chain
import sys
sys.path.insert(0, '/home/santossorianojorge/bettingbot/')
from pipeline.jobs.constants import BetMap
spark = SparkSession.builder .appName('app_name') .master('local[*]') .config('spark.sql.execution.arrow.pyspark.enabled', True) .config('spark.sql.session.timeZone', 'UTC') .config('spark.driver.memory','6G') .config('spark.ui.showConsoleProgress', True) .config('spark.sql.repl.eagerEval.enabled', True) .getOrCreate()


# In[2]:


from google.cloud import storage
storage_client = storage.Client()
bucket = storage_client.get_bucket('bucketbettingbot')
df = spark.read.json(f'gs://bucketbettingbot/pipeline/final/*')


# In[3]:


df.show(200)


# In[ ]:





# In[11]:


from pyspark.sql.functions import to_json, spark_partition_id, collect_list, col, struct

df.select(to_json(struct(*df.columns)).alias("json"))    .groupBy(spark_partition_id())    .agg(collect_list("json").alias("json_list"))    .select(col("json_list").cast("string"))    .write.text(f'gs://bettingbotlast/new/')


# In[ ]:




