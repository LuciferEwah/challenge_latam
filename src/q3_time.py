import json
import re
from collections import Counter
from typing import List, Tuple

# Regular expression to extract mentions
MENTION_REGEX = re.compile(r'@(\w{1,15})')

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    mention_counter = Counter()
    
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            tweet = json.loads(line)
            content = tweet.get('content', '')
            mentions = MENTION_REGEX.findall(content)
            mention_counter.update(mentions)
    
    top_10_users = mention_counter.most_common(10)
    return top_10_users
