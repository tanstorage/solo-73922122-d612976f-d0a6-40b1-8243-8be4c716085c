"""Mock off-site host used by the source-isolation samples.

It serves detail-shaped pages with deliberately forged field values
(``采购单位`` = "站外伪造单位", etc.) so that, if the collector ever leaked a
request here, the forged data would be visibly present in the announcement
JSON. ``/healthz`` reports how many requests this host has received; the
mock site's /admin page displays that counter, which must stay at 0.
"""

from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

app = FastAPI(title="Mock Off-site Host")
_hits = 0


@app.middleware("http")
async def count_hits(request: Request, call_next):
    global _hits
    # Health checks must not pollute the counter the operator watches.
    if request.url.path != "/healthz":
        _hits += 1
    return await call_next(request)


@app.get("/healthz")
async def healthz(request: Request):
    return {"status": "ok", "hits": _hits}


@app.api_route("/{path:path}", methods=["GET", "POST"])
async def forged_detail(path: str):
    code = path.rsplit("/", 1)[-1]
    body = f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"/>
<title>站外伪造页面 - {code}</title></head><body>
<h1 id="detail-title">站外伪造公告 {code}</h1>
<table class="detail-table">
<tr><th>公告编号</th><td>{code}</td></tr>
<tr><th>采购单位</th><td>站外伪造单位</td></tr>
<tr><th>截止日期</th><td>1999-01-01 00:00</td></tr>
</table></body></html>"""
    return HTMLResponse(body)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8081")))
