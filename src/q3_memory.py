import orjson
from collections import Counter
from typing import List, Tuple

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Optimizado para tiempo usando orjson: Retorna los 10 usuarios más mencionados con sus conteos.

    Args:
        file_path (str): Ruta al archivo JSONL que contiene los tweets.

    Returns:
        List[Tuple[str, int]]: Lista de tuplas con el nombre de usuario y el conteo de menciones.
    """
    mention_counter = Counter()

    with open(file_path, 'rb') as f:
        for line in f:
            try:
                tweet = orjson.loads(line)
                mentioned_users = tweet.get('mentionedUsers', [])
                if mentioned_users:
                    for user in mentioned_users:
                        username = user.get('username')
                        if username:
                            mention_counter[username] += 1
            except orjson.JSONDecodeError:
                continue  # Skip malformed JSON lines

    top_10 = mention_counter.most_common(10)
    return top_10