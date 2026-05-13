from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import quote, urlparse


def parse_proxy(proxy: Optional[str]) -> Optional[tuple[Any, ...]]:
    if not proxy or not str(proxy).strip():
        return None
    s = str(proxy).strip()

    if "://" in s:
        u = urlparse(s)
        scheme = (u.scheme or "socks5").lower()
        host = u.hostname
        port = u.port or (1080 if "socks" in scheme else 8080)
        if not host:
            return None
        user = u.username or None
        pwd = u.password or None
        if scheme in ("http", "https"):
            return ("http", host, port, True, user, pwd)
        return ("socks5", host, port, True, user, pwd)

    m = re.match(
        r"^([^:@]+):([^@]+)@([^:]+):(\d+)\s*$",
        s,
    )
    if m:
        user, password, host, port_s = m.groups()
        return ("socks5", host, int(port_s), True, user, password)

    m2 = re.match(r"^([^:]+):(\d+)\s*$", s)
    if m2:
        host, port_s = m2.groups()
        return ("socks5", host, int(port_s), True, None, None)

    return None


def proxy_url_for_httpx(proxy: Optional[str]) -> Optional[str]:
    """Строка прокси как URL для httpx / Bot API (http(s):// или socks5://)."""
    if not proxy or not str(proxy).strip():
        return None
    s = str(proxy).strip()
    if "://" in s:
        return s
    tup = parse_proxy(s)
    if not tup:
        return None
    kind, host, port, _rdns, user, pwd = tup
    scheme = "http" if kind == "http" else "socks5"
    if user is not None:
        auth = f"{quote(str(user), safe='')}:{quote(str(pwd or ''), safe='')}@"
    else:
        auth = ""
    return f"{scheme}://{auth}{host}:{port}"
