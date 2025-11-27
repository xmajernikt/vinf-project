# modify_headers.py
# mitmproxy addon that rotates User-Agent and injects headers per-request

import random
from mitmproxy import http, ctx

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

PER_DOMAIN_HEADERS = {
}

def load(l):
    ctx.log.info("modify_headers addon loaded")

def request(flow: http.HTTPFlow) -> None:

    try:
        ua = random.choice(USER_AGENTS)
        flow.request.headers["User-Agent"] = ua

        for k, v in DEFAULT_HEADERS.items():
            if k not in flow.request.headers:
                flow.request.headers[k] = v

        host = flow.request.host
        domain_headers = PER_DOMAIN_HEADERS.get(host)
        if domain_headers:
            for k, v in domain_headers.items():
                flow.request.headers[k] = v

        flow.request.headers["X-Crawler"] = "VinfCrawlerTomasMajernikSemestralProject/1.0"
    except Exception as e:
        ctx.log.error(f"modify_headers error: {e}")
