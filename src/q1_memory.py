import modin.pandas as mpd
import re
from typing import List, Tuple
from datetime import datetime


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:


    """
    Retrieves the top 10 dates with the most tweets and the most active user for each date using Modin.
    Optimized for execution with distributed processing using Modin.

    Parameters:
    - file_path (str): Path to the JSONL file containing Twitter data.

    Returns:
    - List[Tuple[datetime.date, str]]: List of tuples with date and top username.
    """
    try:
        # Leer el archivo JSONL usando Modin en lugar de pandas
        df = mpd.read_json(file_path, lines=True)
        print("✅ JSONL file read successfully.")

        # Extraer 'username' de la columna 'user'
        df['username_filled'] = df['user'].apply(
            lambda x: x['username'] if isinstance(x, dict) and 'username' in x else 'Unknown'
        )

        # Convertir la columna 'date' a datetime y extraer solo la fecha
        df['date_parsed'] = mpd.to_datetime(df['date'], format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')
        df['date_only'] = df['date_parsed'].dt.date

        # Agrupar por 'date_only' y 'username_filled' y contar tweets
        grouped = df.groupby(['date_only', 'username_filled']).size().reset_index(name='count')

        # Agrupar por 'date_only' para obtener el total de tweets por fecha
        total_tweets = grouped.groupby('date_only')['count'].sum().nlargest(10).reset_index()
    
        # Para cada fecha, obtener el usuario con más tweets
        result = []
        for date in total_tweets['date_only']:
            top_user_df = grouped[grouped['date_only'] == date].nlargest(1, 'count')
            top_user = top_user_df['username_filled'].values[0] if not top_user_df.empty else 'Unknown'
            result.append((date, top_user))
                # Finalizar Ray al terminar
        return result

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []
