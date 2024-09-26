from typing import List, Tuple
from datetime import datetime
import polars as pl

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Retrieves the top 10 dates with the most tweets and the most active user for each date using Polars.
    Optimized for memory usage.
    
    Parameters:
    - file_path (str): Path to the JSONL file containing Twitter data.
    
    Returns:
    - List[Tuple[datetime.date, str]]: List of tuples with date and top username.
    """
    # Read the JSONL file with Polars
    df = pl.read_ndjson(file_path)
    
    # Parse the 'date' column to datetime and extract the date part
    df = df.with_columns([
        pl.col('date').str.strptime(pl.Datetime, fmt="%Y-%m-%dT%H:%M:%S%z").alias('date_parsed')
    ]).with_columns([
        pl.col('date_parsed').dt.date().alias('date_only')
    ])
    
    # Group by 'date_only' and 'user.username', then count the number of tweets
    grouped = df.groupby(['date_only', 'user.username']).agg([
        pl.count().alias('count')
    ])
    
    # Group by 'date_only' to get the total number of tweets per date
    total_tweets = grouped.groupby('date_only').agg([
        pl.sum('count').alias('total')
    ])
    
    # Get the top 10 dates with the most tweets
    top_10_dates = total_tweets.sort('total', reverse=True).head(10)['date_only'].to_list()
    
    result = []
    for date in top_10_dates:
        # Find the user with the maximum tweets on this date
        top_user = grouped.filter(pl.col('date_only') == date)\
                          .sort('count', reverse=True)\
                          .select('user.username')\
                          .first()[0]
        result.append((date, top_user))
    
    return result
