from  pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json
from pyspark.sql.types import *
from itertools import chain
import sys
sys.path.insert(0, '/home/santossorianojorge/bettingbot/')
from constants import BetMap
from google.cloud import storage

spark = SparkSession.builder \
.appName('app_name') \
.master('local[*]') \
.config('spark.sql.execution.arrow.pyspark.enabled', True) \
.config('spark.sql.session.timeZone', 'UTC') \
.config('spark.driver.memory','6G') \
.config('spark.ui.showConsoleProgress', True) \
.config('spark.sql.repl.eagerEval.enabled', True) \
.getOrCreate()

site = 'unibet'
storage_client = storage.Client()
bucket = storage_client.get_bucket('bucketbettingbot')

bucket.delete_blobs([x for x in bucket.list_blobs(prefix=f'pipeline/processed/{site}/')])

df = spark.read.json(f'gs://bucketbettingbot/pipeline/raw/{site}/')

mapping = {'Total Goals - 2nd Half Over/Under': BetMap.TOTAL_GOALS_SECOND_HALF_OU,
 'Asian Total - 1st Half Asian Over/Under': BetMap.ASIAN_TOTAL_FIRST_HALF_OU,
 'Team 2 to win both halves Yes/No': BetMap.TEAM2_WIN_BOTH_HALVES_YN,
 'Team 2 given a Red Card Yes/No': BetMap.TEAM2_RED_CARD_YN,
 'To give an assist Head to Head': BetMap.GIVE_ASSIST_YN,
 'Asian Handicap - 1st Half Asian Handicap': BetMap.ASIAN_HANDICAP_FIRST_HALF_ASIAN_HANDICAP,
 'Team 2 to win at least one half Yes/No': BetMap.TEAM2_WIN_A_HALF_YN,
 'Total Goals Odd/Even Odd/Even': BetMap.TOTAL_GOALS_OE,
 'Total Corners Odd/Even Odd/Even': BetMap.TOTAL_CORNERS_OE,
 'To score & give an assist Head to Head':BetMap.SCORE_AND_ASSIST,
 'Team 2 to win and both teams to score Yes/No':BetMap.TEAM2_WIN_BOTH_TEAMS_SCORE_YN,
 'Goal scored - 0:00-14:59 Yes/No':BetMap.GOAL_SCORED_FROM_0000_TO_1500,
 'Team 1 to win to nil Yes/No': BetMap.TEAM1_WIN_TO_NIL,
 'Team 1 to Score in both halves Yes/No':BetMap.TEAM1_SCORES_BOTH_HALVES,
 'Red Card given Yes/No': BetMap.RED_CARD_GIVEN_YN,
 'Total Corners by Team 2 Over/Under':BetMap.TEAM2_TOTAL_CORNERS_OU,
 'Team 1 given a Red Card Yes/No':BetMap.TEAM1_GIVEN_RED_CARD,
 'Total Goals Over/Under': BetMap.TOTAL_GOALS_OU,
 'Draw No Bet Match':BetMap.DRAW_NO_BET,
 'Total Goals by Team 2 Over/Under': BetMap.TEAM2_TOTAL_GOALS_OU,
 'Draw No Bet - 2nd Half Match':BetMap.DRAW_NO_BET_SECOND_HALF,
 'Total Corners by Team 1 Over/Under':BetMap.TEAM1_TOTAL_CORNERS_OU,
 'Asian Total Asian Over/Under':BetMap.ASIAN_TOTAL_OU,
 'First Goal. No Goal, No Bet Match':BetMap.FIRST_GOAL_NO_GOAL_NO_BET_MATCH,
 'Total Goals by Team 2 - 1st Half Over/Under':BetMap.TEAM2_TOTAL_GOALS_FIRST_HALF_OU,
 'Handicap Handicap':BetMap.HANDICAP,
 'Penalty Kick awarded Yes/No':BetMap.PENALTY_KICK_AWARDED ,
 'Asian Handicap - 1st Half (0 - 0) Asian Handicap':BetMap.ASIAN_HANDICAP_FIRST_HALF_00_AH,
 'Total Goals Odd/Even - 1st Half Odd/Even': BetMap.TOTAL_GOALS_FIRST_HALF_OE,
 'Total Corners - 1st Half Over/Under':BetMap.TOTAL_CORNERS_FIRST_HALF,
 'Total Goals by Team 1 - 1st Half Over/Under':BetMap.TEAM1_TOTAL_GOALS_FIRST_HALF,
 'Total Corners Over/Under':BetMap.TOTAL_CORNERS_OU ,
 'Team 2 to Score in both halves Yes/No':BetMap.TEAM2_SCORE_BOTH_HALVES_YN,
 'Both Teams to Score in both halves Yes/No':BetMap.BOTH_TEAMS_SCORE_BOTH_HALVES_YN,
 'Draw No Bet - 1st Half Match':BetMap.DRAW_NO_BET_FIRST_HALF,
 'Both Teams To Score Yes/No':BetMap.BOTH_TEAMS_SCORE_YN,
 'Both Teams To Score - 1st Half Yes/No':BetMap.BOTH_TEAMS_SCORE_FIRST_HALF_YN,
 'Goal scored - 0:00-29:59 Yes/No':BetMap.GOAL_SCORED_FROM_0000_TO_3000,
 'Total Goals by Team 1 Over/Under':BetMap.TEAM1_TOTAL_GOALS_OU,
 'Draw and both teams to score Yes/No':BetMap.DRAW_BOTH_TEAMS_SCORE_YN,
 'Asian Handicap Asian Handicap':BetMap.ASIAN_HANDICAP,
 'Team 2 To Score - 1st Half Yes/No':BetMap.TEAM2_SCORE_FIRST_HALF_YN,
 'Goal in both halves Yes/No':BetMap.GOAL_BOTH_HALVES_YN,
 'To score from a header Head to Head':BetMap.SCORE_FROM_HEADER,
 'Team 2 to win to nil Yes/No':BetMap.TEAM2_WIN_TO_NIL,
 'Total Cards Over/Under':BetMap.TOTAL_CARDS_OU,
 'Total Goals by Team 1 Odd/Even Odd/Even':BetMap.TEAM1_TOTAL_GOALS_OE,
 'Team 1 to win at least one half Yes/No':BetMap.TEAM1_WIN_A_HALF,
 'Total Goals by Team 2 Odd/Even Odd/Even':BetMap.TEAM2_TOTAL_GOALS_OE,
 'Double Chance Double Chance':BetMap.DOUBLE_CHANCE,
 'To get a Card Head to Head':BetMap.GET_CARD ,
 'Both Teams To Score - 2nd Half Yes/No':BetMap.BOTH_TEAMS_SCORE_SECOND_HALF_YN,
 'Team 1 to win and both teams to score Yes/No': BetMap.TEAM1_WIN_BOTH_SCORE_YN,
 'Total Goals by Team 1 - 2nd Half Over/Under':BetMap.TEAM1_TOTAL_GOALS_SECOND_HALF_OU,
 'Total Goals by Team 2 - 2nd Half Over/Under':BetMap.TEAM2_TOTAL_GOALS_SECOND_HALF_OU,
 'To qualify to the next round Match':BetMap.QUALIFY_NEXT_ROUND,
 'Total Cards - Team 1 Over/Under':BetMap.TEAM1_TOTAL_CARDS_OU ,
 'Asian Handicap (0 - 0) Asian Handicap':BetMap.ASIAN_HANDICAP_00 ,
 'Next Goal, No Goal No Bet (1) Match':BetMap.NEXT_GOAL_NO_GOAL_NO_BET ,
 'Team 1 to win both halves Yes/No':BetMap.TEAM1_WIN_BOTH_HALVES,
 'Handicap - 1st Half Handicap':BetMap.HANDICAP_FIRST_HALF,
 'Total Goals - 1st Half Over/Under':BetMap.TOTAL_GOALS_FIRST_HALF,
 'Team 2 to score first and win the match Yes/No': BetMap.TEAM2_SCORE_FIRST_HALF_AND_WIN_YN,
 'Double Chance - 1st Half Double Chance': BetMap.DOUBLE_CHANCE_FIRST_HALF,
 'Last Goal. No Goal, No Bet Match': BetMap.LAST_GOAL_NO_GOAL_NO_BET,
 'Cards Handicap Handicap': BetMap.CARDS_HANDICAP,
 'To score from a direct free kick Head to Head': BetMap.SCORE_FROM_FREE_KICK_HH,
 'Team 1 to score first and win the match Yes/No': BetMap.TEAM1_TO_SCORE_FIRST_AND_WIN,
 'To score during 1st Half Head to Head': BetMap.SCORE_FIRST_HALF_HH,
 'Team 1 to win after being a goal behind Yes/No': BetMap.TEAM1_TO_WIN_AFTER_GOAL_BEHIND,
 'Team 1 to score from a penalty Yes/No': BetMap.TEAM1_TO_SCORE_FROM_PENALTY,
 'Double Chance - 2nd Half Double Chance': BetMap.DOUBLE_CHANCE_SECOND_HALF,
 'Own goal Yes/No': BetMap.OWN_GOAL_YN,
 'Total Cards - Team 2 Over/Under': BetMap.TEAM2_TOTAL_CARDS_OU,
 'Team 1 to be awarded a penalty Yes/No': BetMap.TEAM1_AWARDED_PENALTY,
 'Team 1 To Score - 1st Half Yes/No': BetMap.TEAM1_TO_SCORE_FIRST_HALF,
 'Team 2 to score from a penalty Yes/No': BetMap.TEAM2_SCORE_FROM_PENALTY,
 'Total Shots on Target by Team 1 Over/Under': BetMap.TEAM1_TOTAL_SHOTS_ON_TARGET_OU,
 'Team 2 to win after being a goal behind Yes/No': BetMap.TEAM2_TO_WIN_AFTER_GOAL_BEHIND,
 'Team 2 to be awarded a penalty Yes/No': BetMap.TEAM2_AWARDED_PENALTY,
 'Total Shots on Target Over/Under': BetMap.TOTAL_SHOTS_ON_TARGET_OU,
 'Total Shots on Target by Team 2 Over/Under': BetMap.TEAM2_TOTAL_SHOTS_ON_TARGET_OU,
 'To Win The Trophy Match': BetMap.TROPHY_WINNER,
 'To score & get booked Head to Head': BetMap.SCORE_AND_BOOKED,
 'Total Shots by Team 1 Over/Under': BetMap.TEAM1_TOTAL_SHOTS_ON_TARGET_OU,
 'To score during 2nd Half Head to Head': BetMap.SCORE_IN_SECOND_HALF,
 'Total Shots Over/Under': BetMap.TOTAL_SHOTS_ON_TARGET_OU,
 'Total Shots by Team 2 Over/Under': BetMap.TEAM2_TOTAL_SHOTS_ON_TARGET_OU,
 'To get a Red Card Head to Head': BetMap.RED_CARD_GIVEN_YN,
 'To miss a penalty Head to Head': BetMap.PENALTY_MISS,
 'To score within the first 14:59 minute Head to Head': BetMap.GOAL_SCORED_FROM_0000_TO_1500
 }
mapping_expr = F.create_map([F.lit(x) for x in chain(*mapping.items())])

def normalise(a):
    return [element / 1000 for element in a if element]

def replace_criterion(string, team1, team2):
    return string.replace(team1, 'Team 1').replace(team2, 'Team 2')

selected_df = (df.withColumn('odds', F.udf(normalise, ArrayType(DoubleType()))(F.col('rawData.outcomes.odds')))
 .withColumn('line', F.udf(normalise, ArrayType(DoubleType()))(F.col('rawData.outcomes.line')))
 .withColumn('line', F.when(F.size(F.col('line')) > 1, F.col('line').cast(StringType())).otherwise(F.col('rawData.outcomes.englishLabel').cast(StringType())))
 .withColumn('criterion', F.udf(replace_criterion, StringType())(F.col('rawData.criterion.englishLabel'), F.col('participant1'), F.col('participant2')))
 .withColumn('criteria',F.concat_ws(" ", F.col('criterion'), F.col('rawData.betOfferType.englishName')))
 .withColumn('criteria', mapping_expr[F.col('criteria')])
 .withColumn('hash', F.hash('participant1', 'participant2', 'competition'))
 .select('sport', 'matchId', 'betId',F.col('rawData.betOfferType.englishName').alias('betType'), 'odds', 'line', 'participant1', 'participant2', 'criteria', 'competition', 'hash', F.lit('unibet').alias('source'))            
 )

selected_df.write.mode('overwrite').format("json").save(f"gs://bucketbettingbot/pipeline/processed/{site}/")

# Delete data
raw_blob = bucket.delete_blobs([x for x in bucket.list_blobs(prefix=f'pipeline/raw/{site}/')])