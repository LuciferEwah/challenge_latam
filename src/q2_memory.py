import modin.pandas as mpd
import re
from typing import List, Tuple


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """Función para obtener los 10 emojis más utilizados con sus respectivos conteos utilizando Modin."""
    try:
        def extract_emojis(text: str) -> List[str]:
            """Function to extract emojis from text."""
            emoji_pattern = re.compile(
            "[" 
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "]+", 
            flags=re.UNICODE
            )
            return emoji_pattern.findall(text) if text else []
        df = mpd.read_json(file_path, lines=True)
        print("✅ JSONL file read successfully.")

        # Directly extract emojis and count frequencies without creating an intermediate column
        emoji_counts = (
            df['content']
            .dropna()  # Drop null values directly
            .map(lambda x: extract_emojis(x))  # Extract emojis
            .explode()  # Explode to separate rows
            .value_counts()  # Count occurrences
            .reset_index()  # Reset index to get a DataFrame
        )

        # Ensure that the correct column names are set
        emoji_counts.columns = ['emoji', 'count']  # Rename columns

        # Get the top 10 emojis
        top_emojis = emoji_counts.nlargest(10, 'count')  # Get the top 10 emojis

        # Convert the result into a list of tuples (emoji, count)
        result = list(zip(top_emojis['emoji'], top_emojis['count']))
        return result

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []