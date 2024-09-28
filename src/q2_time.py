import modin.pandas as mpd
import re
from typing import List, Tuple

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """Función para obtener los 10 emojis más utilizados con sus respectivos conteos utilizando Modin."""
    try:
        def extract_emojis(text: str) -> List[str]:
            """Function to extract emojis from text"""
            # Regular expression to identify emojis
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

        # Read the JSONL file with Modin
        df = mpd.read_json(file_path, lines=True)
        print("✅ JSONL file read successfully.")

        # Filter out null data
        df = df[df['content'].notnull()]

        # Extract emojis and count the frequency of each one
        df['emojis'] = df['content'].map(lambda x: extract_emojis(x))
        emoji_counts = df.explode('emojis').groupby('emojis').size().reset_index(name='count')

        # Get the 10 most frequent emojis
        top_emojis = emoji_counts.nlargest(10, 'count')

        # Convert to a list of tuples
        result = list(zip(top_emojis['emojis'], top_emojis['count']))

        return result

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []
