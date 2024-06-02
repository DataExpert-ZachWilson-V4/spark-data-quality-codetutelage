
from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from collections import namedtuple 

game_details_input = namedtuple("game_details_input", ['game_id', 'team_id', 'team_city', 'player_id', 'player_name'])
game_details_deduped = namedtuple("game_details_deduped", ['game_id', 'team_id', 'team_city', 'player_id', 'player_name', 'row_number'])

def job_1(spark_session):
    input_data = [
    game_details_input(21600215, 1610612739, 202681, 'Kyrie Irving'),
    game_details_input(21600100, 1610612739, 202681, 'Kyrie Irving'),
    game_details_input(21600215, 1610612739, 202681, 'Kyrie Irving'),
    game_details_input(21600100, 1610612739, 2544, 'LeBron James') ]
    
    game_details_expected = [
    game_details_deduped(21600215, 1610612739, 202681, 'Kyrie Irving', 1),
    game_details_deduped(21600100, 1610612739, 202681, 'Kyrie Irving', 1),
    game_details_deduped(21600100, 1610612739, 2544, 'LeBron James', 1)]

    

    df_game_details = spark_session.createDataFrame(input_data)
    print(df_game_details)

    df_game_details.createOrReplaceTempView("df_game_details")

    actual_df = job_1(spark_session, df_game_details)
    expected_df = spark_session.createDataFrame(game_details_expected)

    assert_df_equality(actual_df,expected_df)
