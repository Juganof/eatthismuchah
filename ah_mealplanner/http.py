import time
import urllib.request
from typing import Optional, Tuple


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


def fetch(url: str, delay_s: float = 1.0, user_agent: str = DEFAULT_UA, timeout: int = 20) -> Tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = resp.getcode() or 0
        data = resp.read()
    if delay_s > 0:
        time.sleep(delay_s)
    return status, data

