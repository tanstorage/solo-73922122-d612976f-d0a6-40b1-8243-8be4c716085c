"""Source-isolation rules and ordering tests.

Covers two things the broad end-to-end run cannot pin down deterministically:

1. URL classification: protocol-relative links, userinfo tricks, other ports
   and subdomains are all off-site; normal relative links and same-origin
   absolute links are on-site.
2. Ordering: when the off-site candidate is picked up *first* and blocks its
   announcement number, a same-number on-site row from the other column that
   arrived while the claim was pending must still be collected.

Run with the project venv:
  python -I tests/test_source_isolation_rules.py
"""

import asyncio
import socket
import sys

import uvicorn

sys.path.insert(0, "/workspace/collector")

from app.crawler import Crawler  # noqa: E402

SITE = "http://site.test:8080"


def _crawler() -> Crawler:
    return Crawler(base_url=SITE, sections={"招标公告": "/tenders", "中标公告": "/awards"})


def test_url_classification() -> None:
    crawler = _crawler()
    on_site = [
        "/notice/A-001",
        "notice/A-001",
        "./notice/A-001",
        "?page=2",
        SITE + "/notice/A-001",
        "http://site.test:8080/notice/A-001",
    ]
    off_site = [
        "http://evil.test/notice/A-001",
        "https://site.test:8080/notice/A-001",       # scheme differs
        "http://site.test:8081/notice/A-001",        # port differs
        "http://sub.site.test/notice/A-001",         # host differs
        "http://site.test@evil.test/notice/A-001",   # userinfo trick
        "http://site.test.evil.test/notice/A-001",   # suffix trick
        "//evil.test/notice/A-001",                  # protocol-relative
        "javascript:alert(1)",
        "ftp://site.test/notice/A-001",
    ]
    for href in on_site:
        url = crawler._resolve(href, base_path="/tenders")
        assert crawler._is_same_origin(url), f"{href} -> {url} should be on-site"
    for href in off_site:
        url = crawler._resolve(href, base_path="/tenders")
        assert not crawler._is_same_origin(url), f"{href} -> {url} should be off-site"
    print("url classification OK")


# ---------------------------------------------------------------- ordering
class _Site:
    """Two lists: award names the off-site candidate first, tender is slow."""

    def __init__(self) -> None:
        self.hits: list[str] = []

    async def app(self, scope, receive, send):  # noqa: ANN001
        path = scope["path"]
        query = scope.get("query_string", b"").decode()
        self.hits.append(path + ("?" + query if query else ""))

        if path == "/awards":
            body = self._list("award", href="http://offsite.test/fake/SAME-1")
        elif path == "/tenders":
            # Delay the tender list so the award claim (off-site) is registered
            # first; its row then waits on the pending claim.
            await asyncio.sleep(0.3)
            body = self._list("tender", href="/notice/SAME-1")
        elif path == "/notice/SAME-1":
            body = self._detail("SAME-1", "站内真实单位")
        else:
            await _send(send, 404, "<html>404</html>")
            return
        await _send(send, 200, body)

    @staticmethod
    def _list(section: str, href: str) -> str:
        return (
            '<html><body><table id="announcement-list"><tbody>'
            f'<tr class="announcement"><td class="code">SAME-1</td>'
            f'<td><a href="{href}">公告 SAME-1</a></td></tr>'
            '</tbody></table><nav class="pager" data-total-pages="1"></nav>'
            "</body></html>"
        )

    @staticmethod
    def _detail(code: str, unit: str) -> str:
        return (
            f'<html><body><h1 id="detail-title">标题 {code}</h1>'
            '<table class="detail-table">'
            f"<tr><th>公告编号</th><td>{code}</td></tr>"
            f"<tr><th>采购单位</th><td>{unit}</td></tr>"
            "<tr><th>截止日期</th><td>2026-12-31 17:00</td></tr>"
            "</table></body></html>"
        )


async def _send(send, status: int, body: str) -> None:  # noqa: ANN001
    payload = body.encode()
    await send({"type": "http.response.start", "status": status,
                "headers": [(b"content-type", b"text/html; charset=utf-8")]})
    await send({"type": "http.response.body", "body": payload})


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def test_ordering() -> None:
    site = _Site()
    port = _free_port()

    async def handle(reader, writer):  # noqa: ANN001
        while True:
            line = await reader.readline()
            if not line:
                break
            headers = {}
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
                k, _, v = h.partition(b":")
                headers[k.strip().lower()] = v.strip()
            length = int(headers.get(b"content-length", b"0"))
            if length:
                await reader.readexactly(length)
            method, target, _ = line.split(b" ")
            path, _, query = target.decode().partition("?")

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            collected: dict = {}

            async def send(message):
                if message["type"] == "http.response.start":
                    collected["status"] = message["status"]
                    collected["headers"] = message["headers"]
                else:
                    body = message.get("body", b"")
                    writer.write(b"HTTP/1.1 " + str(collected["status"]).encode() + b" OK\r\n")
                    for k, v in collected["headers"]:
                        writer.write(k + b": " + v + b"\r\n")
                    writer.write(b"content-length: " + str(len(body)).encode() + b"\r\n\r\n")
                    writer.write(body)
                    await writer.drain()
                    writer.close()

            await site.app(
                {"type": "http", "path": path, "query_string": query.encode(),
                 "headers": [], "method": method.decode()},
                receive,
                send,
            )
            return

    server = await asyncio.start_server(handle, "127.0.0.1", port)
    crawler = Crawler(
        base_url=f"http://127.0.0.1:{port}",
        sections={"招标公告": "/tenders", "中标公告": "/awards"},
    )
    try:
        await crawler.start_round()
        await crawler._round_task
        snap = crawler.snapshot()
        records = {r["announcement_code"]: r for r in crawler.records()}
        print(f"ordering: status={snap['status']} records={records} "
              f"unfinished={snap['unfinished']} hits={site.hits}")

        # The off-site candidate was never requested...
        assert not any("offsite.test" in h for h in site.hits), site.hits
        # ...the same-number on-site detail was still collected...
        assert set(records) == {"SAME-1"}, records
        assert records["SAME-1"]["procurement_unit"] == "站内真实单位"
        assert records["SAME-1"]["section"] == "招标公告"
        # ...the off-site row is reported unfinished, and the round is partial.
        offsite = [u for u in snap["unfinished"] if u["section"] == "中标公告"]
        assert len(offsite) == 1 and offsite[0]["attempts"] == 0
        assert snap["status"] == "partial"
        print("ordering OK")
    finally:
        server.close()
        await server.wait_closed()


def main() -> None:
    test_url_classification()
    asyncio.run(test_ordering())
    print("\nALL SOURCE-ISOLATION RULE TESTS PASSED")


if __name__ == "__main__":
    main()
