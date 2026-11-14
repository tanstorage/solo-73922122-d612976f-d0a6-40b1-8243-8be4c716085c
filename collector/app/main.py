"""FastAPI entry point: start / monitor / stop a single collection round."""

from __future__ import annotations

import os
import re
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from .crawler import Crawler

BASE_URL = os.getenv("TARGET_BASE_URL", "http://procurement-site:8080")
# Browser-visible off-site base (rewritten in proxied HTML) and the reachable
# base used by the /site-offsite convenience proxy.
OFFSITE_BASE_URL = os.getenv("OFFSITE_PUBLIC_URL", "http://localhost:8081")
OFFSITE_INTERNAL_URL = os.getenv("OFFSITE_INTERNAL_URL", "http://procurement-offsite:8081")
SECTIONS = {
    "招标公告": os.getenv("SECTION_TENDER_PATH", "/tenders"),
    "中标公告": os.getenv("SECTION_AWARD_PATH", "/awards"),
}

app = FastAPI(title="Procurement Announcement Collector", version="1.0.0")
crawler = Crawler(base_url=BASE_URL, sections=SECTIONS)


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/rounds")
async def start_round() -> dict:
    try:
        round_id = await crawler.start_round()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"round_id": round_id, "state": crawler.snapshot()}


@app.post("/api/rounds/stop")
async def stop_round() -> dict:
    await crawler.stop_round()
    return {"state": crawler.snapshot()}


@app.get("/api/state")
async def get_state() -> dict:
    return crawler.snapshot()


@app.get("/api/records")
async def get_records() -> dict:
    state = crawler.snapshot()
    return {
        "round_id": state["round_id"],
        "status": state["status"],
        "count": len(crawler.records()),
        "records": crawler.records(),
    }


@app.get("/api/records/download")
async def download_records():
    state = crawler.snapshot()
    if state["round_id"] is None:
        raise HTTPException(status_code=404, detail="no collection round has run yet")
    payload = {
        "round_id": state["round_id"],
        "status": state["status"],
        "base_url": state["base_url"],
        "started_at": state["started_at"],
        "finished_at": state["finished_at"],
        "records_count": len(crawler.records()),
        "unfinished_pages": state["unfinished"],
        "records": crawler.records(),
    }
    headers = {
        "Content-Disposition": f'attachment; filename="procurement-{state["round_id"]}.json"'
    }
    return JSONResponse(content=payload, headers=headers)


# Browser entry to the procurement site: same-origin proxy so the operator can
# open both the console and the target site from the collector's port.
_PROXY_CLIENT: httpx.AsyncClient | None = None
_OFFSITE_PROXY_CLIENT: httpx.AsyncClient | None = None

# Root-relative URLs in upstream HTML are rewritten to /site/... so links,
# forms and the off-site sample rows keep working through the proxy. The
# off-site sample host is rewritten to /site-offsite/... which is proxied
# separately: that is a *browser convenience only*, the crawler still refuses
# the off-site origin.
_LINK_ATTRS = re.compile(r'(href|action|src)=("/[^"/][^"]*")')


def _rewrite_site_links(html: str) -> str:
    html = _LINK_ATTRS.sub(lambda m: f'{m.group(1)}="/site{m.group(2)[1:]}', html)
    return html.replace(f'{OFFSITE_BASE_URL.rstrip("/")}/', "/site-offsite/")


@app.on_event("startup")
async def _startup() -> None:
    global _PROXY_CLIENT, _OFFSITE_PROXY_CLIENT
    _PROXY_CLIENT = httpx.AsyncClient(base_url=BASE_URL, timeout=20.0)
    _OFFSITE_PROXY_CLIENT = httpx.AsyncClient(base_url=OFFSITE_INTERNAL_URL, timeout=20.0)


@app.on_event("shutdown")
async def _shutdown() -> None:
    for client in (_PROXY_CLIENT, _OFFSITE_PROXY_CLIENT):
        if client is not None:
            await client.aclose()


async def _forward(client: httpx.AsyncClient, path: str, request: Request):
    # Pass query params exactly once (httpx merges params into the URL).
    proxy_headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    upstream = await client.request(
        request.method,
        path,
        params=list(request.query_params.multi_items()),
        content=await request.body(),
        headers=proxy_headers,
    )
    excluded = {"content-encoding", "transfer-encoding", "connection", "content-length"}
    headers = {k: v for k, v in upstream.headers.items() if k.lower() not in excluded}
    # Keep browser redirects inside the proxy prefix (e.g. the /admin form's
    # 303 back to /admin).
    location = upstream.headers.get("location")
    if location and location.startswith("/") and not location.startswith("//"):
        headers["location"] = f"/site{location}"
    content_type = upstream.headers.get("content-type", "")
    if "application/json" in content_type:
        return JSONResponse(upstream.json(), status_code=upstream.status_code, headers=headers)
    if "text/html" in content_type:
        html = _rewrite_site_links(upstream.text)
        return Response(
            content=html,
            status_code=upstream.status_code,
            headers=headers,
            media_type=content_type,
        )
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=headers,
        media_type=content_type or None,
    )


@app.api_route("/site/{path:path}", methods=["GET", "POST"])
async def site_proxy(path: str, request: Request):
    assert _PROXY_CLIENT is not None
    try:
        return await _forward(_PROXY_CLIENT, path, request)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"procurement site unreachable: {exc}") from exc


@app.api_route("/site-offsite/{path:path}", methods=["GET", "POST"])
async def offsite_proxy(path: str, request: Request):
    """Browser-only view of the mock off-site host.

    Nothing in the collector ever uses this route: the crawler must never
    request the off-site origin, which is exactly what the source-isolation
    samples verify.
    """
    assert _OFFSITE_PROXY_CLIENT is not None
    try:
        return await _forward(_OFFSITE_PROXY_CLIENT, path, request)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"off-site host unreachable: {exc}") from exc


# Browser UI (mounted last so /api routes take precedence).
_static_dir = Path(__file__).resolve().parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="ui")
