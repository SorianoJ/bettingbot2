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
site = 'bwin'
storage_client = storage.Client()
bucket = storage_client.get_bucket('bucketbettingbot')
# Read data
df = spark.read.json(f'gs://bucketbettingbot/pipeline/raw/{site}/')
bucket.delete_blobs([x for x in bucket.list_blobs(prefix=f'pipeline/processed/{site}/')])


# In[4]:


mapping = {
    "Cup Winner" : BetMap.TROPHY_WINNER,
    "Total Goals Team 2 between Half Time and 60:00 min." : BetMap.TEAM2_TOTAL_GOALS_FROM_45_TO_60_MIN,
    "Number of Corners between Half Time and 60:00 min." : BetMap.TOTAL_CORNERS_FROM_4500_TO_6000,
    "Total Goals Team 1 between 30:01 min. and half-time" : BetMap.TEAM_1_TOTAL_GOAL_SCORED_FROM_3000_TO_4500,
    "Team 2 1st Half Total Goals 1,5" : BetMap.TEAM2_TOTAL_GOALS_FIRST_HALF_OU,
    "Number of Goals 1st Half - Odd/Even (no Goal counts as even)" : BetMap.TOTAL_GOALS_FIRST_HALF_OE,
    "Number of Corners between 30:01 min. and Half Time" : BetMap.TOTAL_CORNERS_FROM_3000_TO_4500,
    "Number of Corners (Regular Time)" : BetMap.TOTAL_CORNERS_OU,
    "Total Goals Team 2 between 0 and 15:00 min."  : BetMap.TEAM2_TOTAL_GOALS_FROM_00_TO_15_MIN,
    "Any Team to win exactly by 1 Goal" : BetMap.ONE_GOL_WIN_EXACT,
    "Number of Corners - 2nd Half" : BetMap.TOTAL_CORNERS_SECOND_HALF,
    "Both Teams to Score" : BetMap.BOTH_TEAMS_SCORE_YN,
    "Total Goals O/U - 2nd Half" : BetMap.TOTAL_GOALS_SECOND_HALF_OU,
    "Team 1 - Total Goals - 1st Half " : BetMap.TEAM1_TOTAL_GOALS_FIRST_HALF,
    "Any Team to win by exactly 3 Goals": BetMap.THREE_GOL_WIN_EXACT,
    "Total Goals O/U - Team 2" : BetMap.TEAM2_TOTAL_GOALS_OU,
    "Total Goals  Team 1 between 75:01 min. and full-time" : BetMap.TEAM_1_TOTAL_GOAL_SCORED_FROM_7500_TO_9000,
    "Any Team to win by exactly 2 Goals" : BetMap.TWO_GOL_WIN_EXACT,
    "Total Goals Team 1 between 60:01 and 75:00 min."  : BetMap.TEAM_1_TOTAL_GOAL_SCORED_FROM_6000_TO_7500,
    "Total Goals Team 2 between 30:01 min. and half-time" : BetMap.TEAM2_TOTAL_GOALS_FROM_30_TO_45_MIN,
    "Team 2 to Score" : BetMap.TEAM2_SCORE_YN,
    "Total Goals Team 1 between half-time and 60:00 min." : BetMap.TEAM_1_TOTAL_GOAL_SCORED_FROM_4500_TO_6000,
    "Number of Goals - Odd/Even (no Goal counts as even)" : BetMap.TOTAL_GOALS_OE,
    "Home No Bet" : BetMap.TEAM1_NO_BET,
    "Number of Corners between 15:01 min. and 30:00 min." : BetMap.TOTAL_CORNERS_FROM_1500_TO_3000,
    "Any Team to win by exactly 4 Goals" : BetMap.FOUR_GOL_WIN_EXACT,
    "Total Goals between 75:01 min. and full Time" : BetMap.TOTAL_GOALS_FROM_7500_TO_9000,
    "Total Goals O/U - 1st Half" : BetMap.TOTAL_GOALS_FIRST_HALF_OU,
    "Team 1 1st Half Total Goals 1,5" : BetMap.TEAM1_TOTAL_GOALS_FIRST_HALF,
    "Both Teams To Score 1st Half" : BetMap.BOTH_TEAMS_SCORE_FIRST_HALF_YN,
    "Both Teams to Score 1st Half" : BetMap.BOTH_TEAMS_SCORE_FIRST_HALF_YN,
    "Team 1 to Score" : BetMap.TEAM1_SCORES_YN ,
    "Total Goals between 15:01 min. and 30:00 min." : BetMap.TOTAL_GOALS_FROM_1500_TO_3000,
    "Team 2 to Score in Both Halves" : BetMap.TEAM2_SCORE_BOTH_HALVES_YN,
    "Team 2 No Bet - 1st Half" : BetMap.TEAM2_NO_BET,
    "Total Goals between 75:01 min. and Full Time" : BetMap.TOTAL_GOALS_FROM_7500_TO_9000,
    "Total Goals between 30:01 min. and Half Time" : BetMap.TOTAL_GOALS_FROM_3000_TO_4500,
    "Total Goals" : BetMap.TOTAL_GOALS_OU,
    "Both Teams to Score in 2nd Half" : BetMap.BOTH_TEAMS_SCORE_SECOND_HALF_YN,
    "Number of Corners between 0 min. and 15:00 min." : BetMap.TOTAL_CORNERS_FROM_0000_TO_1500,
    "Number of Corners - 1st Half" : BetMap.TOTAL_CORNERS_FIRST_HALF,
    "Total Goals Team 2 between 15:01 and 30:00 min." : BetMap.TEAM2_TOTAL_GOALS_FROM_15_TO_30_MIN,
    "Team 2 to win to nil" : BetMap.TEAM2_WIN_TO_NIL,
    "Total Goals  Team 1 between 0 and 15:00 min." : BetMap.TEAM_1_TOTAL_GOAL_SCORED_FROM_0000_TO_1500,
    "Team 1 - Clean Sheet" : BetMap.TEAM1_WIN_TO_NIL,
    "Team 1 to Score in Both Halves" : BetMap.TEAM1_SCORES_BOTH_HALVES,
    "Draw No Bet" : BetMap.DRAW_NO_BET,
    "Total Goals  Team 1 between 15:01 and 30:00 min." : BetMap.TEAM_1_TOTAL_GOAL_SCORED_FROM_1500_TO_3000,
    "Total Goals Team 2 between 75:01 min. and full-time" : BetMap.TEAM2_TOTAL_GOALS_FROM_75_TO_90_MIN,
    "Total Goals between 0 min. and 15:00 min." : BetMap.TOTAL_GOALS_FROM_0000_TO_1500,
    "Total Goals between 60:01 min. and 75:00 min." : BetMap.TOTAL_GOALS_FROM_6000_TO_7500,
    "Total Goals O/U - Team 1" : BetMap.TEAM1_TOTAL_GOALS_OU,
    "Team 2 1st Half Total Goals 0,5" : BetMap.TEAM2_TOTAL_GOALS_FIRST_HALF_OU,
    "Number of Corners - Odd/Even" : BetMap.TOTAL_CORNERS_OE,
    "Trophy Winner" : BetMap.TROPHY_WINNER,
    "Number of Corners between 60:01 min. and 75:00 min." : BetMap.TOTAL_CORNERS_FROM_6000_TO_7500,
    "Will team 2 score over/under 0,5 goals?" : BetMap.TEAM2_TOTAL_GOALS_OU,
    "Number of Corners between 75:01 min. and Full Time" : BetMap.TOTAL_CORNERS_FROM_7500_TO_9000,
    "Who will finish higher in the league?" : BetMap.HIGHER_IN_LEAGUE,
    "Any Team to win by exactly 5 Goals" : BetMap.FIVE_GOL_WIN_EXACT,
    "Total Goals Team 2 between 60:01 and 75:00 min." : BetMap.TEAM2_TOTAL_GOALS_FROM_60_TO_75_MIN,
    "Number of Corners 1st Half - Odd/Even": BetMap.TOTAL_CORNERS_FIRST_HALF,
    "Total Goals between Half Time and 60:00 min.": BetMap.TOTAL_GOALS_FROM_4500_TO_6000,
    "Total Goals O/U - Extra Time": BetMap.TOTAL_GOALS_EXTRA_TIME_OU,
    "Both Teams To Score 1st Half":BetMap.BOTH_TEAMS_SCORE_FIRST_HALF_YN,
    "Team 1 To Win And Both Teams To Score":BetMap.TEAM1_WIN_BOTH_SCORE_YN,
    "Team 2 to Win and Both Teams to Score":BetMap.TEAM2_WIN_BOTH_TEAMS_SCORE_YN,
    "Match Won by 2 Goals Exactly":BetMap.TWO_GOL_WIN_EXACT,
    "2nd Half - Total Goals":BetMap.TOTAL_GOALS_SECOND_HALF_OU,
    "Match Won by 1 Goal Exactly":BetMap.ONE_GOL_WIN_EXACT,
    "1st Half - Total Goals":BetMap.TOTAL_GOALS_FIRST_HALF_OU,
    "Team 2 - Clean Sheet":BetMap.TEAM2_WIN_TO_NIL,
    "Team 2 - Total Goals - 1st Half":BetMap.TEAM2_TOTAL_GOALS_FIRST_HALF_OU,
    "Draw No Bet":BetMap.DRAW_NO_BET,
    "Team 2 - Total Goals":BetMap.TEAM2_TOTAL_GOALS_OU,
    "Both Teams To Score 2nd Half":BetMap.BOTH_TEAMS_SCORE_SECOND_HALF_YN,
    "Team 2 - Total Goals - 2nd Half":BetMap.TEAM2_TOTAL_GOALS_SECOND_HALF_OU,
    "Team 1 No Bet":BetMap.TEAM1_NO_BET,
    "Team 2 No Bet - 2nd Half":BetMap.TEAM2_NO_BET,
    "Team 2 No Bet":BetMap.TEAM2_NO_BET,
    "Both Teams to Score Both Halves":BetMap.BOTH_TEAMS_SCORE_BOTH_HALVES_YN,
    "Team 1 - Total Goals ":BetMap.TEAM1_TOTAL_GOALS_OU,
    "2nd Half - Draw No Bet ":BetMap.DRAW_NO_BET,
    "Team 2 To Win And Both Teams To Score":BetMap.TEAM2_WIN_BOTH_TEAMS_SCORE_YN

}
mapping_expr = F.create_map([F.lit(x) for x in chain(*mapping.items())])


# In[5]:


def extract_ou(a):
    return [x.replace('Over ', '').replace('Under ', '').replace(',', '.') for x in a]
def replace_criterion(string, team1, team2):
    return string.replace(team1, 'Team 1').replace(team2, 'Team 2')
selected_df = (df.withColumn('line', F.udf(extract_ou)(F.col('rawData.options.name.value')))
    .withColumn('criterion', F.udf(replace_criterion, StringType())(F.col('rawData.name.value'), F.col('participant1'), F.col('participant2')))
    .withColumn('criteria', mapping_expr[F.col('criterion')])
    .select('line' , 'criteria' , 'rawData.options.price.odds' , 'participant1' , 'participant2' , 'sport' , 'competition' , 'betId','matchId',F.lit('bwin').alias('source') ))


# In[6]:


selected_df.write.mode('overwrite').format("json").save(f"gs://bucketbettingbot/pipeline/processed/{site}/")


# In[7]:


raw_blob = bucket.delete_blobs([x for x in bucket.list_blobs(prefix=f'pipeline/raw/{site}/')])


# In[ ]:




