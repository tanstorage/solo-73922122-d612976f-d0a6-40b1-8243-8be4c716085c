"""End-to-end source-isolation test (real site code, no Docker required).

Starts the real mock procurement site and a counting off-site host on
ephemeral localhost ports, then runs the real :class:`Crawler` against them.

Assertions:

* the off-site host receives **zero** requests (direct links and redirect
  targets are refused before the request is sent);
* every off-site sample is listed as an unfinished detail page with 0
  attempts and the round is ``partial``;
* the same announcement number published on-site in the other column is still
  collected, with genuine on-site data rather than forged off-site fields;
* a normal same-origin relative redirect is followed and collected;
* the deliberately malformed detail page remains unfinished.

Run with the project venv:
  python -I tests/test_source_isolation.py
"""

import asyncio
import importlib.util
import os
import socket
import sys

import uvicorn

os.environ.update(
    {
        "SITE_RATE_ENABLED": "false",
        "SITE_LATENCY_MS": "0",
        "SITE_OFFSITE_SAMPLES": "true",
        "SITE_OFFSITE_BASE": "http://127.0.0.1:1",  # replaced at runtime below
        "SITE_OFFSITE_INTERNAL": "http://127.0.0.1:1",
    }
)

sys.path.insert(0, "/workspace/collector")

from app.crawler import Crawler  # noqa: E402


def _load_package(name: str, path: str):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(path, "__init__.py"), submodule_search_locations=[path]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


site_pkg = _load_package("site_app", "/workspace/site/app")

from fastapi import FastAPI, Request  # noqa: E402
from fastapi.responses import HTMLResponse  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


OFFSITE_PORT = _free_port()
SITE_PORT = _free_port()
OFFSITE_URL = f"http://127.0.0.1:{OFFSITE_PORT}"

# The site renders off-site hrefs from this base; point it at our counter.
site_main = __import__("site_app.main", fromlist=["main"])  # noqa: F401
site_main.OFFSITE_PUBLIC_BASE = OFFSITE_URL
site_main.OFFSITE_INTERNAL_BASE = OFFSITE_URL

offsite_hits: list[str] = []
offsite_app = FastAPI()


@offsite_app.middleware("http")
async def _count(request: Request, call_next):
    offsite_hits.append(f"{request.method} {request.url.path}")
    return await call_next(request)


@offsite_app.get("/healthz")
async def _healthz():
    return {"status": "ok", "hits": len(offsite_hits)}


@offsite_app.api_route("/{path:path}", methods=["GET", "POST"])
async def _forged(path: str):
    code = path.rsplit("/", 1)[-1]
    return HTMLResponse(
        f"<html><body><h1 id='detail-title'>站外伪造 {code}</h1>"
        "<table class='detail-table'>"
        f"<tr><th>公告编号</th><td>{code}</td></tr>"
        "<tr><th>采购单位</th><td>站外伪造单位</td></tr>"
        "<tr><th>截止日期</th><td>1999-01-01 00:00</td></tr>"
        "</table></body></html>"
    )


def _server(app, port: int) -> uvicorn.Server:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    return uvicorn.Server(config)


async def _wait_started(server: uvicorn.Server) -> None:
    while not server.started:
        await asyncio.sleep(0.02)


async def main() -> None:
    site_server = _server(site_main.app, SITE_PORT)
    off_server = _server(offsite_app, OFFSITE_PORT)
    site_task = asyncio.create_task(site_server.serve())
    off_task = asyncio.create_task(off_server.serve())
    await _wait_started(site_server)
    await _wait_started(off_server)

    try:
        crawler = Crawler(
            base_url=f"http://127.0.0.1:{SITE_PORT}",
            sections={"招标公告": "/tenders", "中标公告": "/awards"},
        )
        await crawler.start_round()
        await crawler._round_task
        snap = crawler.snapshot()
        records = {r["announcement_code"]: r for r in crawler.records()}
        unfinished = {(u["section"], u["page"], u["page_type"]) for u in snap["unfinished"]}
        print(
            f"status={snap['status']} records={len(records)} "
            f"offsite_hits={len(offsite_hits)} unfinished={sorted(unfinished)}"
        )

        # 1. No request ever reached the off-site host.
        assert not offsite_hits, f"off-site host received requests: {offsite_hits}"

        # 2. Off-site samples are unfinished details; the off-site target was
        #    never requested. Direct links cost 0 attempts; the redirect sample
        #    legitimately requests the same-origin /relay hop first, then stops
        #    before the cross-origin hop (so exactly 1 attempt, no more).
        offsite_rows = {
            u["page"]: u for u in snap["unfinished"]
            if u["page"] in {"ZB-2026-005", "ZB-2026-007", "ZB-2026-015"}
        }
        assert set(offsite_rows) == {"ZB-2026-005", "ZB-2026-007", "ZB-2026-015"}
        for code, row in offsite_rows.items():
            assert row["page_type"] == "detail"
            expected_attempts = 1 if code == "ZB-2026-007" else 0
            assert row["attempts"] == expected_attempts, row
            assert "origin" in row["reason"], row["reason"]
        assert "/relay/" in offsite_rows["ZB-2026-007"]["url"]

        # 3. Round is partial and the off-site pages are visible.
        assert snap["status"] == "partial", snap["status"]

        # 4. Same-number on-site details were collected, not skipped.
        for code in ("ZB-2026-005", "ZB-2026-007", "ZB-2026-015"):
            assert code in records, f"{code} missing from records"
            assert "伪造" not in records[code]["procurement_unit"], records[code]
            assert records[code]["detail_url"].startswith(f"http://127.0.0.1:{SITE_PORT}/")

        # 5. The reverse-column case (award direct link) was collected on-site.
        assert records["ZB-2026-015"]["section"] == "招标公告"

        # 6. Normal same-origin relative redirect still followed (the list row
        #    points at /relay-local/..., which 302s to /notice/...).
        assert "ZB-2026-014" in records
        assert records["ZB-2026-014"]["detail_url"].endswith("/notice/ZB-2026-014")

        # 7. Broken detail page still unfinished (3 attempts).
        broken = [u for u in snap["unfinished"] if u["page"] == "ZB-2026-010"]
        assert broken and broken[0]["attempts"] == 3, broken

        # 8. Duplicate on-site numbers still skipped as before.
        assert snap["pages_failed"] == 4, snap["pages_failed"]  # 3 off-site + 1 broken

        print("source-isolation checks OK")
    finally:
        site_server.should_exit = True
        off_server.should_exit = True
        await asyncio.gather(site_task, off_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
