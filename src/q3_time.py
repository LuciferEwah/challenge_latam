
from typing import List, Tuple
import polars as pl
from polars.exceptions import PolarsError

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Optimizado para uso de memoria con Polars: Devuelve los 10 usuarios más mencionados con sus respectivos conteos.

    Argumentos:
        file_path (str): Ruta al archivo JSONL que contiene los tweets.

    Retorna:
        List[Tuple[str, int]]: Lista de tuplas con el nombre de usuario y el número de menciones.

    """
    try:
        # Definir el esquema para asegurar tipos de datos consistentes
        schema = {
            "mentionedUsers": pl.List(pl.Struct([
                pl.Field("username", pl.Utf8)
            ]))
        }

        # Leer el archivo JSONL con Polars y aplicar el esquema
        df = pl.read_ndjson(file_path, schema=schema, ignore_errors=True)
        print("✅ Archivo JSONL leído exitosamente.")

        # Explode para separar cada mención de usuario en filas individuales
        exploded_df = df.explode("mentionedUsers")

        # Extraer la columna 'username' de la estructura mencionada
        username_df = exploded_df.with_columns([
            pl.col("mentionedUsers").struct.field("username").alias("username")
        ]).drop_nulls("username")

        # Agrupar por 'username' y contar menciones
        mention_counts = (
            username_df
            .group_by("username")
            .agg(pl.count("username").alias("mention_count"))
            .sort("mention_count", descending=True)  # Ordenar por el número de menciones
            .limit(10)  # Obtener solo los 10 primeros
        )

        # Convertir el resultado a una lista de tuplas
        top_10 = list(mention_counts.iter_rows())
        
        return top_10

    except PolarsError as pe:
        print(f"❌ Error al procesar los datos con Polars: {pe}")
        return []
    except FileNotFoundError:
        print(f"❌ Error: El archivo {file_path} no se encuentra en el directorio especificado.")
        return []
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return []

