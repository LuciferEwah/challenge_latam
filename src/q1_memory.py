import dask.dataframe as dd
import pandas as pd
import json
from dask.distributed import Client
import time
from memory_profiler import memory_usage

# Inicia un cliente Dask (para paralelización)
client = Client()

# Define la función que procesa el archivo JSON con Dask
def q1_memory(file_path: str):
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
            tweet_count = top_user_df['count'].values[0]
            result.append((date, top_user, tweet_count))

        result_df = pd.DataFrame(result, columns=["Fecha", "Usuario con más tweets", "Número de tweets"])
        return result_df

    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return pd.DataFrame()

