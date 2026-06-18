# scanner_core.py — UPGRADE existing scan_stocks()

import time
from sector_rotation import get_sector_performance_multi

# Cache sector data separately (refreshes every 5 min)
_sector_cache = {}
_sector_cache_time = 0

def get_sector_data_cached():
    global _sector_cache, _sector_cache_time
    if time.time() - _sector_cache_time > 300:  # 5 min cache
        try:
            _sector_cache = get_sector_performance_multi(['1d'])
            _sector_cache_time = time.time()
        except Exception:
            _sector_cache = {'1d': {}}
    return _sector_cache


