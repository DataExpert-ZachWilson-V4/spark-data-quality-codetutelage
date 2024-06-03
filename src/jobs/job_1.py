from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1() -> str:
    return """
    WITH nba_values as (
      SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) as rnum
      FROM bootcamp.nba_game_details
    )
    SELECT 
      game_id,
      team_id,
      team_abbreviation,
      team_city,
      player_id,
      player_name,
      nickname,
      start_position,
      comment,
      min,
      fgm
    FROM nba_values 
    WHERE rnum=1
    """

def job_1(spark_session: SparkSession) -> Optional[DataFrame]:
    query = query_1()
    return spark_session.sql(query)

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("job_1").getOrCreate()
    output_df = job_1(spark)
    output_df.show()
    spark.stop()
