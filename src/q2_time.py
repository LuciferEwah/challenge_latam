import pandas as pd
import re
from typing import List, Tuple
import dask.dataframe as dd
import re
from typing import List, Tuple



def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """Function that gets the top 10 most used emojis with their counts using Dask."""
    try:
        def extract_emojis(text: str) -> List[str]:
            """Función para extraer emojis de un texto"""
            # Expresión regular para identificar emojis
            emoji_pattern = re.compile(
                "[" 
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                "]+", 
                flags=re.UNICODE
            )
            return emoji_pattern.findall(text)
        # Read the JSONL file with Dask specifying the number of partitions
        df = dd.read_json(file_path, lines=True, dtype={'content': 'object'}, blocksize='16MB')
        print("✅ JSONL file read successfully.")
        
        # Filter out null data
        df = df[df['content'].notnull()]

        # Specify the expected output type for map_partitions
        emoji_meta = pd.Series(dtype='object')  # This is the expected output type for the emojis

        # Extract emojis and count the frequency of each one
        emoji_counts = (
            df['content']
            .map_partitions(lambda part: part.apply(lambda x: extract_emojis(x)), meta=emoji_meta)
            .explode()
            .value_counts()
            .reset_index()
        )

        # Rename the columns
        emoji_counts.columns = ['emoji', 'count']

        # Get the 10 most frequent emojis
        top_emojis = emoji_counts.head(10)

        # Convert to a list of tuples
        result = list(zip(top_emojis['emoji'], top_emojis['count']))

        return result

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []