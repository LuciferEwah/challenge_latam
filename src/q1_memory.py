from typing import List, Tuple
from datetime import datetime
import dask.dataframe as dd
from dask.distributed import Client

# Inicia un cliente Dask (para paralelización)
client = Client()

# Define la función que procesa el archivo JSON con Dask
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Función que procesa un archivo JSONL para obtener las 10 fechas con más tweets
    y el usuario con más tweets en esas fechas. Optimizada para ejecución en Dask.

    Parameters:
    - file_path (str): Ruta al archivo JSONL.

    Returns:
    - List[Tuple[datetime.date, str]]: Lista de tuplas con la fecha y el usuario con más tweets.
    """
    try:
        # Lee el archivo JSONL
        df = dd.read_json(file_path, lines=True)
        print("✅ Archivo JSONL leído exitosamente.")

        # Extrae el username y procesa la columna 'date'
        df['username_filled'] = df['user'].map(lambda x: x.get('username', 'Unknown'), meta=('username_filled', 'object'))
        df['date_parsed'] = dd.to_datetime(df['date'], format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')
        df['date_only'] = df['date_parsed'].dt.date

        # Agrupar por 'date_only' y 'username_filled', y contar tweets por usuario y fecha
        grouped = df.groupby(['date_only', 'username_filled']).size().compute()

        # Convertir el resultado a DataFrame de pandas para manipularlo
        grouped_df = grouped.reset_index().rename(columns={0: 'count'})

        # Agrupar por 'date_only' para obtener el total de tweets por fecha
        total_tweets = grouped_df.groupby('date_only')['count'].sum().nlargest(10).reset_index()

        # Para cada fecha, obtener el usuario con más tweets
        result = []
        for date in total_tweets['date_only']:
            top_user_df = grouped_df[grouped_df['date_only'] == date].nlargest(1, 'count')
            top_user = top_user_df['username_filled'].values[0]
            result.append((date, top_user))

        return result

    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return []

# Ejemplo de uso
file_path = "ruta/al/archivo/farmers-protest-tweets-2021-2-4.json"
result = q1_memory(file_path)

# Mostrar resultados
for date, user in result:
    print(f"Fecha: {date}, Usuario con más tweets: {user}")
