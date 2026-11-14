# 公共采购公告采集系统

面向招标数据员的公告采集交付件：FastAPI 采集器 + 带限流/延迟开关的本地模拟采购站，
Docker Compose 一键启动，浏览器即可启动采集、查看实时进度、停止任务并下载 JSON。

## 目录结构

```
.
├── docker-compose.yml          # 三服务编排：procurement-site + procurement-offsite + collector
├── collector/                  # 采集器（FastAPI + httpx + BeautifulSoup）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # API、JSON 下载、采购站同源代理、静态页挂载
│       ├── crawler.py          # 采集引擎：并发闸门、429 全站冷却、停止回收、来源隔离
│       ├── parser.py           # BeautifulSoup 列表/详情解析（失败抛 ParseError）
│       └── static/index.html   # 浏览器控制台（单文件，原生 JS）
├── site/                       # 本地模拟公共采购站
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # 栏目/详情页、滑动窗口限流、延迟开关、越站样例、管理页
│       ├── data.py             # 确定性内存数据集（跨栏目重复编号 + 坏详情页 + 越站样例）
│       └── offsite.py          # 模拟“站外站点”：伪造字段 + 请求计数（应为 0）
└── tests/                      # 契约/来源隔离测试与解析器单测（标准库 + venv 依赖）
```

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 采集控制台 | http://localhost:8000/ | 启动/停止/进度/事件/下载 JSON |
| 采购站（同源代理入口） | http://localhost:8000/site/ | 浏览器入口，经采集器反代到站点 |
| 采购站限流/样例管理 | http://localhost:8000/site/admin | 限流阈值、窗口、延迟毫秒、越站样例开关 |
| 越站目标（仅浏览器查看） | http://localhost:8000/site-offsite/fake/ZB-2026-005 | 站外页面观感，采集器不会访问 |
| 采购站直连 | http://localhost:8080/ | 仅排障时使用 |
| 越站目标直连/计数 | http://localhost:8081/healthz | `hits` 为站外累计请求数（采集器应保持 0） |

## 采集规则与实现对照

| 需求 | 实现 |
| --- | --- |
| 固定站点两个栏目，遍历分页列表及详情 | `Crawler` 配置 `/tenders`、`/awards`；首页解析 `data-total-pages` 后入队其余列表页，再逐条入队详情页 |
| **详情直链只能请求配置站点的同源地址** | 列表行 href 先按浏览器规则解析，再校验 scheme/host/port；越站直链不入队、不发请求，直接记为未完成（attempts=0） |
| **每次跳转只能请求同源地址，越站前不得发请求或收录内容** | 关闭 httpx 自动重定向，`_fetch_with_redirects` 手动逐跳跟随；每跳先校验同源再发请求，跨站目标立即终止（`OffSiteBlocked`），响应体不解析 |
| 保留正常站内相对跳转 | `/notice/...`、`relay-local/...` 等相对/同源绝对跳转照常跟随（上限 5 跳） |
| **越站候选记为未完成，不占用公告编号** | 越站直链在入队前即标记未完成、不写入去重表；跳转越站则把该编号的“租约”释放给同号站内候选 |
| **同号站内详情随后仍可采录** | 新增按编号的采集租约 `CodeClaim`：同号候选先挂起等待，租约成功/失败才记为重复跳过；被越站拦截（或停止前未执行）时租约转交，等待中的站内候选重新入队采集 |
| 按公告编号去重 | `_code_claims` 跨栏目去重；同编号只采一次详情，跳过计数在分栏目进度中可见 |
| 提取标题、采购单位、截止日期 | `parser.parse_detail_page`，缺任一字段即 `ParseError` |
| 可下载 JSON | `GET /api/records/download`，附件文件名带轮次 ID |
| 单进程同时只采一轮 | 单 uvicorn worker + `round_lock`；重复启动返回 `409` |
| 最多两个请求在途 | 全站共享 `asyncio.Semaphore(2)`，两个栏目共用；重定向的每一跳都占用同一闸门 |
| 429 按 Retry-After 整数秒全站暂停 | `_extend_cooldown` 维护单一冷却钟，`_pass_gate` 拦截所有后续请求（含另一栏目与重试）；后续 429 只延长不缩短；已在途请求允许收尾 |
| 每页最多尝试三次 | 列表/详情统一 `_get_parsed_page`，传输错误、4xx/5xx、解析失败均消耗尝试次数；越站拦截为终态，不消耗重试 |
| 耗尽/解析失败须列出未完成页，不能报整轮成功 | 失败页与越站页进入 `unfinished`；轮次结束状态为 `partial` 而非 `completed` |
| 停止阻止排队/冷却请求，关闭在途请求并回收连接后才显示已停止 | `stop_round` 先置停止位，再 `aclose()` 关闭在途请求与连接池，`await` 轮次协程收尾后状态才变 `stopped`，随后重建客户端允许新轮 |
| 旧响应不能写入新轮 | 每轮生成 `round_id`，冷却唤醒与回写前校验；新轮重建客户端、状态与记录容器 |

轮次状态机：`idle → running → completed | partial | stopped`（停止过程中短暂为 `stopping`）。

### 来源隔离细节

- 判据是 **scheme + host + port**，与配置的 `TARGET_BASE_URL` 精确一致；
  `//evil.test/x`、`http://site.test@evil.test/x`、`http://site.test.evil.test/x`、
  其他端口/子域/协议均判为越站。
- 越站直链：解析阶段即拦截，**零请求**，未完成原因说明目标 origin，`attempts=0`。
- 越站跳转：站内 hop 正常请求（占用 1 次尝试），拿到 302 后先解析 `Location`
  并校验；跨站则立即终止，**不请求越站地址**，`attempts=1`。
- 公告编号租约：同号的两条候选只有一条拥有租约。被越站拦截的候选会把租约
  交给等待中的同号站内候选（并补计入队计数），因此该编号仍能被正常采录；
  采录成功后，其余同号行按重复跳过。


## 模拟采购站说明

- 数据集：36 条招标公告（6 个列表页）+ 24 条中标公告（4 个列表页），
  中标公告复用前 24 个招标编号，用于验证跨栏目去重；
- `ZB-2026-010` 详情页故意缺少“采购单位/截止日期”，用于验证三次解析失败后
  进入未完成页面列表、整轮标记为 `partial`；
- 限流为滑动窗口，默认 10 秒窗口 12 个请求，超限返回 `429` 与整数 `Retry-After: 5`；
- `/admin` 可运行时切换限流开关、窗口容量、窗口秒数与每请求延迟毫秒，
  保存即重置计数，也可单独重置计数。

### 来源隔离样例（`/admin` 可切换）

| 编号 | 所属栏目 | 越站形式 | 同号站内详情 | 期望结果 |
| --- | --- | --- | --- | --- |
| `ZB-2026-005` | 招标公告 | 列表链接为绝对 URL，直指站外 `procurement-offsite` | 中标公告 `/notice/ZB-2026-005` | 招标行未完成（0 次请求），中标行正常采录 |
| `ZB-2026-007` | 招标公告 | 列表链接为站内 `/relay/...`，该地址 302 跳往站外 | 中标公告 `/notice/ZB-2026-007` | 招标行未完成（1 次尝试），中标行正常采录 |
| `ZB-2026-015` | 中标公告 | 列表链接为绝对 URL，直指站外（反向列序） | 招标公告 `/notice/ZB-2026-015` | 中标行未完成，招标行正常采录 |
| `ZB-2026-014` | 招标公告 | 对照：站内相对 302 → `/notice/ZB-2026-014` | — | 正常跟随并采录 |

站外目标 `site/app/offsite.py` 提供的伪造详情页含“站外伪造单位”等字段，
便于肉眼确认这些内容从未进入公告 JSON；其 `/healthz` 的 `hits` 计数在
`/admin` 上展示，采集器运行时应为 0（浏览器手动点击 `/site-offsite/...` 会 +1）。
两个越站开关关闭后，列表行恢复为普通站内 `/notice/...` 链接。

## 推荐演示流程

1. `docker compose up --build`，打开 http://localhost:8000/ ；
2. 直接点“启动一轮采集”：状态为 `partial`——35 条记录；未完成列表 4 条：
   `ZB-2026-005` / `ZB-2026-015`（越站直链，0 次请求）、`ZB-2026-007`
   （站内 302 越站，1 次尝试）、`ZB-2026-010`（坏详情页，3 次尝试）；
3. 打开 http://localhost:8000/site/admin ：越站目标累计请求应为 0；
   勾掉“越站直链/站内跳转越站样例”再跑一轮，4 条未完成只剩坏详情页；
4. 在 `/site/admin` 把窗口容量调到 3：再启动一轮，观察控制台“全站冷却剩余”倒计时，
   冷却期间两个栏目均无新请求完成；
5. 冷却进行中点“停止”：状态在 1 秒内变为 `stopped`，排队/冷却中的页面进入未完成列表；
6. 立即再启动一轮：状态与记录完全重置，证明旧轮响应不会写入新轮；
7. 点“下载 JSON”获取结构化结果（含未完成页清单与 `detail_url`）。

## 本地测试（无需 Docker）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r collector/requirements.txt -r site/requirements.txt
.venv/bin/python -I tests/test_parser.py
.venv/bin/python -I tests/test_crawler_contract.py
.venv/bin/python -I tests/test_source_isolation.py
.venv/bin/python -I tests/test_source_isolation_rules.py
```

- `test_crawler_contract.py` 用内存假上游验证：在途请求峰值恰为 2、429 后整站等待
  不少于 Retry-After 且重试可恢复、冷却中停止后零请求泄漏、停止后新轮干净无旧数据。
- `test_source_isolation.py` 用真实站点代码 + 计数型站外服务跑完整一轮：站外
  `hits == 0`、越站行进入未完成、同号站内行被采录（无伪造字段）、站内相对跳转正常、
  坏详情页仍为 3 次尝试、整轮 `partial`。
- `test_source_isolation_rules.py` 校验 URL 分类（协议相对、userinfo、端口、子域等）
  与关键时序：越站候选先占号时，等待中的同号站内候选仍会被补采。
