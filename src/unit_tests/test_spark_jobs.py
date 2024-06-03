
from chispa.dataframe_comparer import *
from src.jobs.job_1 import job_1
from collections import namedtuple 
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark_session():
    return (
      SparkSession.builder
      .master("local")
      .appName("chispa")
      .getOrCreate()
  )


def test_job_1(spark_session):
    # Define the schema using namedtuple
    NBARecord = namedtuple('NBARecord', [
        'game_id', 'team_id', 'team_abbreviation', 'team_city', 
        'player_id', 'player_name', 'nickname', 'start_position', 
        'comment', 'min', 'fgm'
    ])

    # Create the input data using namedtuple
    input_data = [
        NBARecord(1, 10, 'TA', 'CityA', 100, 'PlayerA', 'NickA', 'G', 'Good', '30', 5),
        NBARecord(1, 10, 'TA', 'CityA', 100, 'PlayerA', 'NickA', 'G', 'Good', '30', 5),
        NBARecord(2, 20, 'TB', 'CityB', 200, 'PlayerB', 'NickB', 'F', 'Bad', '20', 3)
    ]
    input_df = spark_session.createDataFrame(input_data)

    # Register the DataFrame as a temporary view
    input_df.createOrReplaceTempView("nba_game_details")

    # Define the expected schema using namedtuple
    ExpectedNBARecord = namedtuple('ExpectedNBARecord', [
        'game_id', 'team_id', 'team_abbreviation', 'team_city', 
        'player_id', 'player_name', 'nickname', 'start_position', 
        'comment', 'min', 'fgm'
    ])

    # Create the expected output data using namedtuple
    expected_data = [
        ExpectedNBARecord(1, 10, 'TA', 'CityA', 100, 'PlayerA', 'NickA', 'G', 'Good', '30', 5),
        ExpectedNBARecord(2, 20, 'TB', 'CityB', 200, 'PlayerB', 'NickB', 'F', 'Bad', '20', 3)
    ]
    expected_df = spark_session.createDataFrame(expected_data)

    # Run the job_1 function
    output_df = job_1(spark_session,'nba_game_details')

    # Compare the output DataFrame with the expected DataFrame
    assert_df_equality(output_df, expected_df, ignore_row_order=True)
