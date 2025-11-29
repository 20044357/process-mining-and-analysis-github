import re
from datetime import datetime
from typing import Optional

def parse_hour(hour_string: str) -> Optional[datetime]:
    if not re.match(r"^\d{4}-\d{2}-\d{2}-\d{1,2}$", hour_string):
        return None

    parts = hour_string.split('-')
    hour_part = parts[-1]
    if len(hour_part) == 2 and hour_part.startswith('0'):
        return None

    try:
        parsed = datetime.strptime(hour_string, "%Y-%m-%d-%H")
    except ValueError:
        return None

    if parsed.year < 2000 or parsed.year > 2100:
        return None

    return parsed