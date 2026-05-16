import itertools
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class ProxyRotator:
    """Round-robin proxy rotation with optional health tracking."""

    def __init__(self):
        raw = os.getenv("PROXY_LIST", "")
        self._proxies = [p.strip() for p in raw.split(",") if p.strip()]
        self._cycle = itertools.cycle(self._proxies) if self._proxies else None
        self._bad: set[str] = set()
        if self._proxies:
            logger.info("ProxyRotator: %d proxies loaded", len(self._proxies))
        else:
            logger.info("ProxyRotator: no proxies configured — using direct connection")

    def next(self) -> Optional[str]:
        if not self._cycle:
            return None
        for _ in range(len(self._proxies)):
            proxy = next(self._cycle)
            if proxy not in self._bad:
                return proxy
        logger.warning("All proxies marked bad — falling back to direct connection")
        return None

    def mark_bad(self, proxy: str) -> None:
        self._bad.add(proxy)
        logger.warning("Proxy marked bad: %s (%d/%d bad)", proxy, len(self._bad), len(self._proxies))

    @property
    def has_proxies(self) -> bool:
        return bool(self._proxies)
