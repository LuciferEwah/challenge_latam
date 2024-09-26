import json
from collections import defaultdict
from typing import List, Tuple
from emoji import demojize, UNICODE_EMOJI_ENGLISH
import heapq

def extract_emojis(text: str) -> List[str]:
    return [char for char in text if char in UNICODE_EMOJI_ENGLISH]

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    emoji_counter = defaultdict(int)
    
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            tweet = json.loads(line)
            content = tweet.get('content', '')
            emojis = extract_emojis(content)
            for emoji_char in emojis:
                emoji_counter[emoji_char] += 1
    
    # Use a heap to find top 10 emojis
    top_10_emojis = heapq.nlargest(10, emoji_counter.items(), key=lambda x: x[1])
    return top_10_emojis
