import modin.pandas as mpd
import re
from typing import List, Tuple
from datetime import datetime


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Recupera las 10 fechas con más tweets y el usuario más activo para cada fecha utilizando Modin.
    Optimizado para ejecución con procesamiento distribuido usando Modin.

    Parámetros:
    - file_path (str): Ruta al archivo JSONL que contiene los datos de Twitter.

    Retorna:
    - List[Tuple[datetime.date, str]]: Lista de tuplas con la fecha y el usuario más activo.
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
