"""Crawler engine.

Concurrency and rate-limit contract:

* Only one collection round may run at a time (``round_lock``).
* At most two HTTP requests are in flight at any moment, across both columns
  (a single shared ``asyncio.Semaphore(2)``).
* A 429 with a ``Retry-After`` integer header pauses the *whole site*: every
  later request, including the retry itself and requests of the other column,
  waits in :meth:`_pass_gate`. Further 429s only extend the pause. Requests
  already in flight are allowed to finish.
* Every page (list or detail) is attempted at most three times. Exhausted
  retries and permanently unparseable pages are recorded as unfinished, so a
  round is never reported as successful in that case.
* Source isolation: every request (the direct detail link as well as every
  redirect hop) is resolved and checked against the configured site origin.
  A cross-origin link is never enqueued or requested; a cross-origin redirect
  target is never requested either. Both are recorded as unfinished without
  claiming the announcement code, so the same code published in the other
  column at a same-origin URL can still be collected. Redirects that stay on
  the configured origin are followed normally.
* Announcement codes are claimed when a same-origin detail job is queued, not
  when the row is first seen. A duplicate row waits until the claim settles;
  if the claim fails (including an off-site redirect), a different same-origin
  candidate for that code is allowed to run, so a failed or blocked candidate
  never permanently occupies the code.
* Stop blocks queued/cooling requests from being sent, closes in-flight
  requests and the connection pool, and only then flips the state to stopped
  and permits a new round. A round id guards against a late response from the
  old round being written into the new one.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit

import httpx

from . import parser
from .parser import ListParseResult, ParseError

MAX_ATTEMPTS = 3
MAX_IN_FLIGHT = 2
MAX_REDIRECTS = 5
GATE_POLL_SECONDS = 0.25
EVENT_LOG_LIMIT = 200
DEFAULT_RETRY_AFTER_SECONDS = 5
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class RoundStopped(Exception):
    """Raised inside a round when cooperative stop has been requested."""


class OffSiteBlocked(Exception):
    """The candidate URL or a redirect target is outside the configured origin.

    Terminal by design: the request is not sent (and, for a redirect hop, the
    off-site target is not requested), so retrying would be pointless.
    """


@dataclass
class SectionProgress:
    list_total_pages: int = 0
    list_completed: int = 0
    list_failed: int = 0
    detail_enqueued: int = 0
    detail_completed: int = 0
    detail_failed: int = 0
    duplicated_skipped: int = 0


@dataclass
class UnfinishedPage:
    section: str
    page_type: str  # "list" or "detail"
    page: str  # list page number, or announcement code for detail pages
    url: str
    reason: str
    attempts: int


@dataclass
class CodeClaim:
    """Ownership of an announcement code inside one round.

    Exactly one same-origin detail job owns the code at a time. Rows from the
    other column do not block a worker: they wait in ``waiters`` until the
    owner settles. On success/failure the code is consumed and waiters are
    counted as duplicates; when the owner is blocked off-site (or was stopped
    before running), the code is released and the waiters are re-queued, so a
    blocked candidate never occupies the number.
    """

    state: str = "pending"  # pending | done | failed | blocked
    waiters: list[dict] = field(default_factory=list)


@dataclass
class RoundState:
    round_id: str | None = None
    status: str = "idle"  # idle | running | stopping | stopped | completed | partial
    base_url: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    stop_requested: bool = False
    pages_enqueued: int = 0
    pages_completed: int = 0
    pages_failed: int = 0
    records_count: int = 0
    cooldown_remaining_seconds: float = 0.0
    sections: dict[str, SectionProgress] = field(default_factory=dict)
    unfinished: list[UnfinishedPage] = field(default_factory=list)
    events: list[str] = field(default_factory=list)


class Crawler:
    def __init__(self, base_url: str, sections: dict[str, str], timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        # section display name -> list path
        self.sections = sections
        self.timeout = timeout
        # Only scheme/host/port matter: every request must stay on this origin.
        base_parts = urlsplit(self.base_url)
        self._origin = (base_parts.scheme.lower(), (base_parts.hostname or "").lower(),
                        base_parts.port or (443 if base_parts.scheme == "https" else 80))

        self._round_lock = asyncio.Lock()
        self._round_task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

        # Per-round runtime flags (reset when a round starts).
        self._round_id: str | None = None
        self._stop_event = asyncio.Event()
        self._inflight = asyncio.Semaphore(MAX_IN_FLIGHT)

        # Site-wide 429 cooldown (one clock for the whole crawler).
        self._cooldown_until = 0.0

        self.state = RoundState(
            base_url=self.base_url,
            sections={name: SectionProgress() for name in sections},
        )
        self._records: dict[str, dict] = {}
        self._code_claims: dict[str, CodeClaim] = {}

    # ------------------------------------------------------------------ state
    def snapshot(self) -> dict:
        now = time.monotonic()
        remaining = max(0.0, self._cooldown_until - now)
        self.state.cooldown_remaining_seconds = round(remaining, 1)
        return asdict(self.state)

    def records(self) -> list[dict]:
        return list(self._records.values())

    def _log(self, message: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.state.events.append(f"{stamp} {message}")
        del self.state.events[:-EVENT_LOG_LIMIT]

    # ------------------------------------------------------------- lifecycle
    async def start_round(self) -> str:
        async with self._round_lock:
            if self._round_task is not None and not self._round_task.done():
                raise RuntimeError("a collection round is already running")

            round_id = uuid.uuid4().hex[:12]
            self._round_id = round_id
            self._stop_event = asyncio.Event()
            self._inflight = asyncio.Semaphore(MAX_IN_FLIGHT)
            self._cooldown_until = 0.0
            self._records: dict[str, dict] = {}
            self._code_claims = {}

            self.state = RoundState(
                round_id=round_id,
                status="running",
                base_url=self.base_url,
                started_at=datetime.now(timezone.utc).isoformat(),
                sections={name: SectionProgress() for name in self.sections},
            )
            # Limits created before this round do not apply to the new one:
            # a fresh client also guarantees a fresh connection pool.
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers={"User-Agent": "procurement-collector/1.0"},
                # Redirects are followed manually so every hop can be checked
                # against the configured origin before it is requested.
                follow_redirects=False,
            )
            self._log(f"round {round_id} started; columns={list(self.sections)}")
            self._round_task = asyncio.create_task(self._run_round(round_id))
            return round_id

    async def stop_round(self) -> None:
        async with self._round_lock:
            task = self._round_task
            client = self._client
            if task is None or task.done():
                return

            self._stop_event.set()
            self.state.stop_requested = True
            self.state.status = "stopping"
            self._log("stop requested: queued/cooling requests will not be sent")

            # Close in-flight requests and release the connection pool. The
            # round coroutine catches the resulting errors and finishes.
            if client is not None:
                try:
                    await client.aclose()
                except Exception:  # pragma: no cover - defensive
                    pass

            # Wait until the round coroutine has fully wound down.
            try:
                await task
            except asyncio.CancelledError:  # pragma: no cover - defensive
                raise
            except Exception:
                pass

            # Rebuild a closed client so the next round owns a clean pool.
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers={"User-Agent": "procurement-collector/1.0"},
                follow_redirects=False,
            )
            self._log("in-flight requests closed and connections recycled; round stopped")

    # ----------------------------------------------------------------- round
    async def _run_round(self, round_id: str) -> None:
        # Mutable holder so both the coordinator and workers can track the
        # number of jobs queued or being processed.
        pending = {"count": 0}
        queue: asyncio.Queue = asyncio.Queue()
        for name, path in self.sections.items():
            queue.put_nowait(
                {"section": name, "kind": "list", "page": 1, "url": path, "attempts": 0}
            )
            pending["count"] += 1
            self.state.pages_enqueued += 1

        workers = [
            asyncio.create_task(self._worker(round_id, queue, i, pending))
            for i in range(MAX_IN_FLIGHT)
        ]

        try:
            # Coordinator: finish once every job (and every job it spawned)
            # has been settled. Stop interrupts the wait promptly.
            while pending["count"] > 0 and not self._stop_event.is_set():
                await asyncio.sleep(0.1)
            for w in workers:
                queue.put_nowait(None)
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            for w in workers:
                if not w.done():
                    w.cancel()
            for w in workers:
                try:
                    await w
                except asyncio.CancelledError:
                    pass
            # Any job still queued when the round ends is unfinished.
            while not queue.empty():
                leftover = queue.get_nowait()
                if isinstance(leftover, dict):
                    self._mark_unfinished(
                        leftover, "round stopped before request was sent", leftover["attempts"]
                    )
            # Rows held behind an unresolved code claim are unfinished too.
            for claim in self._code_claims.values():
                for waiter in claim.waiters:
                    self._mark_unfinished(
                        waiter, "round stopped before request was sent", waiter["attempts"]
                    )
                claim.waiters.clear()
            # Close the client used by this round (idempotent if stopped).
            if self._client is not None:
                try:
                    await self._client.aclose()
                except Exception:
                    pass

        if round_id != self._round_id:
            # A new round superseded this one; do not touch its state.
            return

        self.state.finished_at = datetime.now(timezone.utc).isoformat()
        if self._stop_event.is_set():
            self.state.status = "stopped"
        elif self.state.pages_failed:
            self.state.status = "partial"
        else:
            self.state.status = "completed"
        self.state.records_count = len(self._records)
        self._log(
            f"round ended: status={self.state.status}, "
            f"records={self.state.records_count}, unfinished={len(self.state.unfinished)}"
        )

    async def _worker(
        self, round_id: str, queue: asyncio.Queue, worker_no: int, pending: dict
    ) -> None:
        while True:
            job = await queue.get()
            try:
                if job is None:
                    return
                if self._stop_event.is_set():
                    self._mark_unfinished(job, "round stopped before request was sent", job["attempts"])
                    pending["count"] -= 1
                    return
                try:
                    if job["kind"] == "list":
                        await self._handle_list(round_id, job, queue, pending)
                    else:
                        await self._handle_detail(round_id, job, queue, pending)
                except RoundStopped:
                    # Stop while waiting/cooling: no further request is sent.
                    self._mark_unfinished(job, "round stopped during cooldown/queue wait", job["attempts"])
                    pending["count"] -= 1
                    return
            finally:
                queue.task_done()

    # -------------------------------------------------------------- handlers
    async def _handle_list(
        self, round_id: str, job: dict, queue: asyncio.Queue, pending: dict
    ) -> None:
        section = job["section"]
        progress = self.state.sections[section]
        try:
            result: ListParseResult = await self._get_parsed_page(
                round_id, job, parser.parse_list_page
            )
        except RoundStopped:
            raise
        except OffSiteBlocked as exc:
            progress.list_failed += 1
            self.state.pages_failed += 1
            self._mark_unfinished(job, str(exc), job["attempts"])
            self._log(f"[{section}] list page {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return
        except Exception as exc:
            progress.list_failed += 1
            self.state.pages_failed += 1
            self._mark_unfinished(job, str(exc), MAX_ATTEMPTS)
            self._log(f"[{section}] list page {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return

        progress.list_completed += 1
        self.state.pages_completed += 1

        spawned = 0
        if job["page"] == 1:
            progress.list_total_pages = result.total_pages
            for page_no in range(2, result.total_pages + 1):
                new_job = {
                    "section": section,
                    "kind": "list",
                    "page": page_no,
                    "url": self._list_url(section, page_no),
                    "attempts": 0,
                }
                queue.put_nowait(new_job)
                spawned += 1
                self.state.pages_enqueued += 1

        for item in result.items:
            item_url = self._resolve(item.url, base_path=self._section_path(section))
            if not self._is_same_origin(item_url):
                # Off-site candidate: never request it, never let it claim the
                # announcement number. It is recorded as unfinished instead.
                self._mark_unfinished(
                    {
                        "section": section,
                        "kind": "detail",
                        "page": item.code,
                        "url": item_url,
                    },
                    OffSiteBlocked(
                        "detail link points to another origin "
                        f"({self._origin_label()} required); no request was sent"
                    ).__str__(),
                    attempts=0,
                )
                progress.detail_failed += 1
                self.state.pages_failed += 1
                self._log(
                    f"[{section}] detail {item.code} unfinished: "
                    f"off-site link {self._describe_url(item_url)} was not requested"
                )
                continue

            detail_job = {
                "section": section,
                "kind": "detail",
                "page": item.code,
                "url": item_url,
                "base_path": self._section_path(section),
                "attempts": 0,
            }
            claim = self._code_claims.get(item.code)
            if claim is not None and claim.state == "pending":
                # Another column already owns this code. Hold the row without
                # occupying a worker; it is re-queued if that owner is blocked.
                detail_job["claim"] = claim
                claim.waiters.append(detail_job)
                pending["count"] += 1
                self._log(
                    f"[{section}] detail {item.code} held: code already being collected"
                )
                continue
            if claim is not None and claim.state in ("done", "failed"):
                # The code was already collected (or terminally failed).
                progress.duplicated_skipped += 1
                continue

            # No owner yet, or the previous owner was blocked off-site: this
            # same-origin candidate may claim the number.
            claim = CodeClaim()
            self._code_claims[item.code] = claim
            detail_job["claim"] = claim
            queue.put_nowait(detail_job)
            spawned += 1
            progress.detail_enqueued += 1
            self.state.pages_enqueued += 1

        # This job is settled; the jobs it spawned keep the round alive.
        pending["count"] += spawned - 1

    async def _handle_detail(
        self, round_id: str, job: dict, queue: asyncio.Queue, pending: dict
    ) -> None:
        section = job["section"]
        progress = self.state.sections[section]
        claim: CodeClaim = job["claim"]
        try:
            if not self._is_same_origin(job["url"]):
                raise OffSiteBlocked("detail link outside configured origin")
            detail = await self._get_parsed_page(round_id, job, parser.parse_detail_page)
        except RoundStopped:
            # The coordinator drains the rows held behind this claim in the
            # round finalizer, so leave ownership untouched here.
            raise
        except OffSiteBlocked as exc:
            # A cross-origin redirect hop is terminal: the code is released so
            # a same-origin candidate for it can still run.
            self._settle_claim(job["page"], claim, "blocked", queue, pending)
            progress.detail_failed += 1
            self.state.pages_failed += 1
            self._mark_unfinished(job, str(exc), job["attempts"])
            self._log(f"[{section}] detail {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return
        except Exception as exc:
            self._settle_claim(job["page"], claim, "failed", queue, pending)
            progress.detail_failed += 1
            self.state.pages_failed += 1
            self._mark_unfinished(job, str(exc), MAX_ATTEMPTS)
            self._log(f"[{section}] detail {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return

        progress.detail_completed += 1
        self.state.pages_completed += 1
        self._records[detail["code"]] = {
            "announcement_code": detail["code"],
            "title": detail["title"],
            "procurement_unit": detail["unit"],
            "deadline": detail["deadline"],
            "section": section,
            "detail_url": job.get("final_url", job["url"]),
        }
        self.state.records_count = len(self._records)
        self._settle_claim(job["page"], claim, "done", queue, pending)
        pending["count"] -= 1

    # ---------------------------------------------------------- source policy
    def _section_path(self, section: str) -> str:
        return self.sections[section]

    def _resolve(self, url: str, base_path: str | None = None) -> str:
        """Resolve a link exactly as a browser would, against the site origin.

        Relative links (``/notice/A``, ``notice/A``, ``?page=2``) stay on the
        configured origin. Absolute links keep their own origin so they can be
        checked and rejected before any request is sent. ``base_path`` may be
        a site-relative path or an absolute same-origin URL (redirect target).
        """
        url = (url or "").strip()
        base = self.base_url + "/"
        if base_path:
            if base_path.startswith(("http://", "https://")):
                base = base_path
            else:
                # A relative href resolves against the page's directory path.
                base = self._join_path(base_path)
        return urljoin(base, url)

    def _join_path(self, path: str) -> str:
        return self.base_url + "/" + path.lstrip("/")

    def _is_same_origin(self, url: str) -> bool:
        try:
            parts = urlsplit(url)
            port = parts.port or (443 if parts.scheme.lower() == "https" else 80)
        except ValueError:
            return False
        return (
            parts.scheme.lower() in ("http", "https")
            and parts.scheme.lower() == self._origin[0]
            and (parts.hostname or "").lower() == self._origin[1]
            and port == self._origin[2]
        )

    def _origin_label(self) -> str:
        scheme, host, port = self._origin
        default_port = 443 if scheme == "https" else 80
        return f"{scheme}://{host}" + ("" if port == default_port else f":{port}")

    def _settle_claim(
        self, code: str, claim: CodeClaim, state: str, queue, pending: dict | None
    ) -> None:
        """Finish ownership of ``code`` and resolve rows waiting on it.

        * ``done``/``failed``: the code is consumed; waiters are counted as
          duplicates and released.
        * ``blocked``: the owning candidate never produced a same-origin
          result, so ownership passes to the first waiter (which is queued),
          or the code is released if there are no waiters.

        ``queue``/``pending`` are ``None`` while a round is being torn down;
        waiters are left in ``claim.waiters`` for the finalizer to drain.
        """
        if claim.state != "pending":
            return
        claim.state = state
        if state == "blocked" and claim.waiters:
            if queue is None:
                return  # finalizer drains the rest
            next_job = claim.waiters.pop(0)
            self._code_claims[code] = CodeClaim(state="pending", waiters=claim.waiters)
            next_job["claim"] = self._code_claims[code]
            queue.put_nowait(next_job)
            # The promoted row now becomes a real request: count it as enqueued.
            self.state.sections[next_job["section"]].detail_enqueued += 1
            self.state.pages_enqueued += 1
            self._log(
                f"code {code} released after off-site block; "
                f"retrying same-origin candidate [{next_job['section']}]"
            )
            return

        if state == "blocked":
            # No waiter: leave the code unclaimed for any later row.
            self._code_claims[code] = CodeClaim(state="blocked")
            return

        if queue is None:
            return
        for waiter in claim.waiters:
            self.state.sections[waiter["section"]].duplicated_skipped += 1
        if pending is not None:
            pending["count"] -= len(claim.waiters)
        if claim.waiters:
            self._log(
                f"code {code} {state}; skipped {len(claim.waiters)} duplicate row(s)"
            )
        claim.waiters.clear()

    # ------------------------------------------------------------- transport
    async def _get_parsed_page(self, round_id: str, job: dict, parse_func):
        """Fetch one URL with up to three attempts, then parse it.

        Redirects are followed manually: every hop is resolved and checked
        against the configured origin *before* it is requested. A redirect
        that leaves the origin aborts the page immediately with
        :class:`OffSiteBlocked`; no request is sent to the off-site target and
        its body is never parsed. Same-origin relative/absolute redirects are
        followed normally (bounded by ``MAX_REDIRECTS``).

        Transport errors and HTTP error statuses (including 429 after cooldown)
        consume attempts; a page whose HTML never parses within three attempts
        surfaces the last :class:`ParseError`.
        """
        last_error: Exception | None = None
        while job["attempts"] < MAX_ATTEMPTS:
            job["attempts"] += 1
            await self._pass_gate(round_id)  # may raise RoundStopped
            if round_id != self._round_id:
                raise RoundStopped("round superseded before request")
            try:
                async with self._inflight:
                    response, final_url = await self._fetch_with_redirects(round_id, job)
            except httpx.RequestError as exc:
                last_error = exc
                if self._stop_event.is_set():
                    raise RoundStopped("in-flight request closed by stop")
                self._log(
                    f"request error ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}: {exc}"
                )
                continue

            # The request itself has finished: 429 only gates *later* traffic.
            if response.status_code == 429:
                wait_seconds = self._retry_after_seconds(response)
                self._extend_cooldown(wait_seconds)
                last_error = httpx.HTTPStatusError(
                    f"429 Too Many Requests; site-wide cooldown {wait_seconds}s",
                    request=response.request,
                    response=response,
                )
                self._log(
                    f"429 on {job['url']}; whole site pauses {wait_seconds}s "
                    f"(attempt {job['attempts']}/{MAX_ATTEMPTS})"
                )
                continue

            if response.status_code >= 400:
                last_error = httpx.HTTPStatusError(
                    f"HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                self._log(
                    f"HTTP {response.status_code} ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}"
                )
                continue

            try:
                result = parse_func(response.text)
            except ParseError as exc:
                last_error = exc
                self._log(
                    f"parse error ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}: {exc}"
                )
                continue
            job["final_url"] = final_url
            return result

        raise last_error if last_error else ParseError("unknown failure")

    async def _fetch_with_redirects(
        self, round_id: str, job: dict
    ) -> tuple[httpx.Response, str]:
        """Request ``job['url']`` and follow same-origin redirects manually.

        The origin check happens before each hop, so an off-site target never
        receives a request. Raises :class:`OffSiteBlocked` as soon as a hop
        would leave the configured origin.
        """
        current = self._resolve(job["url"], base_path=job.get("base_path"))
        for _hop in range(MAX_REDIRECTS + 1):
            if not self._is_same_origin(current):
                raise OffSiteBlocked(
                    f"redirect/link leaves site origin ({self._origin_label()} required): "
                    f"{self._describe_url(current)}"
                )
            if self._stop_event.is_set() or round_id != self._round_id:
                raise RoundStopped("stop requested before redirect hop")
            assert self._client is not None
            response = await self._client.get(current)

            if response.status_code not in REDIRECT_STATUSES:
                return response, current

            location = response.headers.get("Location", "").strip()
            if not location:
                return response, current  # caller reports the status
            target = self._resolve(location, base_path=current)
            if self._is_same_origin(target):
                self._log(
                    f"same-origin redirect {self._describe_url(response.request.url)} -> "
                    f"{self._describe_url(target)}"
                )
            else:
                self._log(
                    f"redirect {self._describe_url(response.request.url)} -> "
                    f"{self._describe_url(target)} leaves site origin; not requested"
                )
            current = target

        raise ParseError(f"too many redirects (>{MAX_REDIRECTS})")

    def _describe_url(self, url) -> str:
        """Short, log-safe rendering of a URL (no query string payload)."""
        parts = urlsplit(str(url))
        return f"{parts.scheme}://{parts.netloc}{parts.path}"

    async def _pass_gate(self, round_id: str) -> None:
        """Block while stop is requested or a site-wide cooldown is active."""
        while True:
            if self._stop_event.is_set() or round_id != self._round_id:
                raise RoundStopped("stop requested before request could be sent")
            remaining = self._cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.sleep(min(GATE_POLL_SECONDS, remaining))
            except asyncio.CancelledError:  # pragma: no cover - defensive
                raise

    def _extend_cooldown(self, seconds: int) -> None:
        # Later 429s can only extend the existing wait.
        self._cooldown_until = max(self._cooldown_until, time.monotonic() + seconds)

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> int:
        raw = response.headers.get("Retry-After", "").strip()
        if raw.isdigit():
            return max(0, int(raw))
        return DEFAULT_RETRY_AFTER_SECONDS

    # ---------------------------------------------------------------- helpers
    def _list_url(self, section: str, page: int) -> str:
        path = self.sections[section]
        separator = "&" if "?" in path else "?"
        return f"{path}{separator}page={page}"

    def _absolute(self, url: str) -> str:
        if url.startswith(("http://", "https://")):
            return url
        return f"{self.base_url}/{url.lstrip('/')}"

    def _mark_unfinished(self, job: dict, reason: str, attempts: int) -> None:
        page_type = job["kind"]
        self.state.unfinished.append(
            UnfinishedPage(
                section=job["section"],
                page_type=page_type,
                page=str(job["page"]),
                url=self._absolute(job["url"]),
                reason=reason,
                attempts=attempts,
            )
        )
