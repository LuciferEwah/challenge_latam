from typing import List, Tuple
from datetime import datetime
import polars as pl
from polars.exceptions import PolarsError


def q1_time(file_path: str) -> pl.DataFrame:
    """
    Retrieves the top 10 dates with the most tweets and the most active user for each date using Polars.
    Optimized for memory usage.

    Parameters:
    - file_path (str): Path to the JSONL file containing Twitter data.

    Returns:
    - pl.DataFrame: DataFrame containing date and top username for each of the top 10 dates.
    """
    try:
        # Definir el esquema para asegurar tipos de datos consistentes
        schema = {
            "date": pl.Utf8,
            "user": pl.Struct([
                pl.Field("username", pl.Utf8)
            ]),
            "content": pl.Utf8
        }

        # Leer el archivo JSONL con Polars, ignorando registros malformados
        df = pl.read_ndjson(file_path, schema=schema, ignore_errors=True)
        print("✅ Archivo JSONL leído exitosamente.")
        
        # Extraer 'username' del struct 'user', rellenando valores nulos con 'Unknown'
        df = df.with_columns([
            pl.col('user').struct.field('username').fill_null('Unknown').alias('username_filled')
        ])
        print("✅ Campo 'username' procesado correctamente.")
        
        # Liberar la columna 'user' ya que ya no es necesaria
        df = df.drop(['user'])
        
        # Parsear la columna 'date' a datetime y extraer solo la fecha
        df = df.with_columns([
            pl.col('date').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False).alias('date_parsed')
        ]).drop('date')  # Eliminar la columna original 'date' después de la conversión
        print("✅ Columna 'date' parseada correctamente.")

        # Extraer solo la fecha (YYYY-MM-DD)
        df = df.with_columns([
            pl.col('date_parsed').dt.date().alias('date_only')
        ]).drop('date_parsed')  # Eliminar 'date_parsed' ya que solo necesitamos 'date_only'

        # Agrupar por 'date_only' y 'username_filled' y contar el número de tweets
        grouped = df.group_by(['date_only', 'username_filled']).agg([
            pl.len().alias('count')  # Usar pl.len() en lugar de pl.count()
        ])
        del df  # Liberar 'df' ya que solo necesitamos 'grouped' ahora
        print("\n✅ Agrupación por 'date_only' y 'username_filled' realizada correctamente.")

        # Agrupar por 'date_only' para obtener el total de tweets por fecha
        total_tweets = grouped.group_by('date_only').agg([
            pl.sum('count').alias('total')
        ])
        print("✅ Agrupación por 'date_only' para obtener totales realizada correctamente.")

        # Obtener las 10 fechas con más tweets
        top_10_dates = total_tweets.sort('total', descending=True).head(10)['date_only'].to_list()
        del total_tweets  # Liberar 'total_tweets' ya que solo necesitamos las fechas
        print(f"✅ Top 10 fechas con más tweets: {top_10_dates}")

        # Para cada fecha, obtener el usuario con más tweets
        result = []
        for date in top_10_dates:
            top_user_df = grouped.filter(pl.col('date_only') == date)\
                                 .sort('count', descending=True)\
                                 .select(['username_filled', 'count'])\
                                 .head(1)

            if top_user_df.height > 0:
                top_user = top_user_df[0, 'username_filled']
                tweet_count = top_user_df[0, 'count']
                result.append((date, top_user, tweet_count))
            else:
                result.append((date, 'Unknown', 0))
        
        # Liberar 'grouped' ya que no es necesario después de obtener los resultados
        del grouped
        
        # Crear un DataFrame con los resultados, especificando la orientación de las filas
        result_df = pl.DataFrame(result, schema=["Fecha", "Usuario con más tweets", "Número de tweets"], orient="row")

        return result_df

    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return pl.DataFrame()
