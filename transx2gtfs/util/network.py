from datetime import timedelta, datetime
from pathlib import Path
import urllib.parse

from filelock import FileLock
import urllib.request

_CACHE_KEY = "transx2gtfs"
_CACHE_DIR = Path.home() / ".cache" / _CACHE_KEY
_CACHE_LOCK = _CACHE_DIR / ".lock"


def download_cached(url: str, name: str | None = None, *, max_age: timedelta = timedelta(days=30)) -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if name is None:
        name = urllib.parse.urlparse(url).path.rsplit("/", maxsplit=1)[1]
    cached_file = _CACHE_DIR / name

    def cached_file_is_good() -> bool:
        if not cached_file.is_file():
            return False
        
        mtime = datetime.fromtimestamp(cached_file.stat().st_mtime)
        return datetime.now() - mtime <= max_age

    if not cached_file_is_good():
        with FileLock(_CACHE_LOCK):
            if cached_file_is_good():
                return cached_file
            tmp = _CACHE_DIR / f"{name}.tmp"
            print(f"Retrieving {name} from {url}...")
            urllib.request.urlretrieve(url, tmp)
            tmp.rename(cached_file)
    
    return cached_file
