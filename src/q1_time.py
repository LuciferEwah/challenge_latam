from typing import List, Tuple
from datetime import datetime
import polars as pl
from polars.exceptions import PolarsError

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Retrieves the top 10 dates with the most tweets and the most active user for each date using Polars.
    Optimized for execution time.

    Parameters:
    - file_path (str): Path to the JSONL file containing Twitter data.

    Returns:
    - List[Tuple[datetime.date, str]]: List of tuples with date and top username.
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

        # Leer el archivo JSONL con Polars para optimización de tiempo
        df = pl.read_ndjson(file_path, schema=schema, ignore_errors=True)
        print("✅ Archivo JSONL leído exitosamente.")
        
        # Extraer 'username' y procesar la columna 'date'
        df = df.with_columns([
            pl.col('user').struct.field('username').fill_null('Unknown').alias('username_filled'),
            pl.col('date').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False).alias('date_parsed')
        ]).with_columns([
            pl.col('date_parsed').dt.date().alias('date_only')
        ])

        # Agrupar por 'date_only' y 'username_filled' y contar tweets por usuario y fecha
        grouped = df.group_by(['date_only', 'username_filled']).agg([
            pl.len().alias('count')
        ])

        # Agrupar por 'date_only' para obtener el total de tweets por fecha y seleccionar las 10 principales fechas
        total_tweets = grouped.group_by('date_only').agg([
            pl.sum('count').alias('total')
        ]).sort('total', descending=True).limit(10)

        # Obtener las 10 fechas con más tweets
        top_10_dates = total_tweets['date_only'].to_list()

        # Ahora para cada una de las fechas, obtener el usuario con más tweets
        result = []
        for date in top_10_dates:
            top_user = grouped.filter(pl.col('date_only') == date)\
                              .sort('count', descending=True)\
                              .select(['username_filled', 'count'])\
                              .head(1)

            if top_user.height > 0:
                result.append((date, top_user[0, 'username_filled']))
            else:
                result.append((date, 'Unknown'))

        return result

    except PolarsError as pe:
        print(f"❌ Error al procesar los datos con Polars: {pe}")
        return []
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return []
