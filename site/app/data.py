"""Deterministic in-memory dataset for the mock procurement site.

Source-isolation samples (switchable from /admin):

* ``ZB-2026-005`` in the tender column links straight to an off-site host;
  the award column publishes the same number at a same-origin detail page.
* ``ZB-2026-007`` in the tender column links to a same-origin path that
  redirects off-site; the award column has the same number on-site.
* ``ZB-2026-015`` in the award column is a direct off-site link, while the
  tender column's same number stays on-site (the reverse column order).
* ``ZB-2026-014`` (tender) goes through a normal same-origin redirect.
"""

import random
from dataclasses import dataclass

PAGE_SIZE = 6
# This announcement's detail page is intentionally malformed so the crawler's
# parse-failure / unfinished-page path can be demonstrated end to end.
BROKEN_DETAIL_CODE = "ZB-2026-010"

# code -> (section whose list row uses the sample, kind)
#   "direct"   = the list row's href is an absolute URL on another origin
#   "redirect" = the list row's href is same-origin but 302s to another origin
# The code always also exists in the other column at a normal on-site URL.
OFFSITE_SAMPLES: dict[str, tuple[str, str]] = {
    "ZB-2026-005": ("tender", "direct"),
    "ZB-2026-007": ("tender", "redirect"),
    "ZB-2026-015": ("award", "direct"),
}

# Same-origin relative redirect that must keep working normally.
RELAY_SAMPLE_CODE = "ZB-2026-014"

_UNITS = [
    "市教育局装备中心", "区城市管理局", "县人民医院", "省交通规划研究院",
    "市政园林管理处", "市大数据发展局", "区公共资源交易中心", "市应急管理局",
    "县水务集团", "市图书馆",
]
_GOODS = [
    "办公设备采购项目", "智慧校园平台建设", "道路养护工程", "医疗影像设备采购",
    "云算力服务租赁", "绿化苗木采购", "应急物资储备", "数据中心机房改造",
    "档案数字化加工", "食品安全检测服务", "视频监控系统升级", "物业服务采购",
]


@dataclass
class Announcement:
    code: str
    title: str
    unit: str
    deadline: str
    section: str  # tender | award


def _build() -> dict[tuple[str, str], Announcement]:
    rng = random.Random(20261113)
    data: dict[tuple[str, str], Announcement] = {}
    # 36 tender announcements -> 6 list pages at PAGE_SIZE=6.
    for i in range(1, 37):
        code = f"ZB-2026-{i:03d}"
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        data[("tender", code)] = Announcement(
            code=code,
            title=f"{rng.choice(_GOODS)}（招标编号 {code}）",
            unit=rng.choice(_UNITS),
            deadline=f"2026-{month:02d}-{day:02d} 17:00",
            section="tender",
        )
    # 24 award announcements reuse tender codes on purpose: the same
    # announcement number appears in both columns so the collector's
    # code-based deduplication is exercised during a real run.
    for i in range(1, 25):
        code = f"ZB-2026-{i:03d}"
        original = data[("tender", code)]
        data[("award", code)] = Announcement(
            code=code,
            title=original.title.replace("（招标编号", "（中标公告，原招标编号"),
            unit=original.unit,
            deadline=original.deadline,
            section="award",
        )
    return data


ANNOUNCEMENTS: dict[tuple[str, str], Announcement] = _build()


def section_rows(section: str) -> list[Announcement]:
    return [a for key, a in ANNOUNCEMENTS.items() if key[0] == section]


def find_by_code(code: str) -> Announcement | None:
    # Detail pages are shared by code across the two columns.
    for item in ANNOUNCEMENTS.values():
        if item.code == code:
            return item
    return None


def sample_for(code: str, section: str) -> str | None:
    """Return the off-site sample kind for this row, if any (direct|redirect)."""
    sample = OFFSITE_SAMPLES.get(code)
    if sample is None or sample[0] != section:
        return None
    return sample[1]


def detail_path(section: str, code: str, offsite_base: str, enabled: dict) -> str:
    """Href rendered for a list row, honouring the off-site sample switches.

    ``enabled`` maps sample kind -> bool. Disabled samples fall back to the
    normal same-origin detail so the site still behaves.
    """
    if section == "tender" and code == RELAY_SAMPLE_CODE:
        # Control sample: a normal same-origin relative redirect that the
        # collector must keep following.
        return f"/relay-local/{code}"
    sample = sample_for(code, section)
    if sample == "direct" and enabled.get("direct"):
        return f"{offsite_base.rstrip('/')}/fake/{code}"
    if sample == "redirect" and enabled.get("redirect"):
        return f"/relay/{code}"
    return f"/notice/{code}"
