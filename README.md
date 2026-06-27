# PingMon

PingMon 是一个轻量的分布式 TCP 可用性监控工具，由两个 Go 程序组成：

- `supervisor`：下发探测任务，接收结果，保存到 SQLite，并提供 Web Dashboard。
- `agent`：定时拉取任务，并发执行 TCP 连接探测，再批量上报结果。

支持多 Agent、IPv4/IPv6、实时刷新、历史查询、节点状态、数据聚合与自动清理。两个程序均为独立二进制，不依赖外部数据库。

## 快速开始

需要 Go 1.25 或更高版本。

```bash
go run ./cmd/supervisor -config configs/supervisor.toml
```

另开一个终端启动 Agent：

```bash
go run ./cmd/agent -config configs/agent.toml
```

打开 <http://127.0.0.1:8080/dashboard>。示例账号为 `admin`，密码为 `change-me`；正式部署前必须修改。

## 配置

Supervisor 和 Agent 均支持 JSON、TOML。完整示例见：

- [`configs/supervisor.toml`](configs/supervisor.toml)
- [`configs/agent.toml`](configs/agent.toml)

### Supervisor

常用配置：

| 字段 | 说明 | 示例值 |
| --- | --- | --- |
| `listen` | HTTP 监听地址 | `:8080` |
| `sqlite_path` | SQLite 数据库路径 | `data/pingmon.db` |
| `dashboard_user` | Dashboard 和管理 API 用户名 | `admin` |
| `dashboard_password` | Dashboard 和管理 API 密码 | `change-me` |
| `agent_token` | Agent API 的 Bearer Token；留空表示不认证 | `""` |
| `raw_retention_days` | 原始结果保留天数 | `30` |
| `retention_days` | 聚合结果保留天数 | `365` |
| `rollup_interval_minutes` | 历史聚合粒度 | `60` |
| `failure_threshold` | 连续异常多少轮后标记连接故障 | `3` |

`[params]` 控制每轮次数、探测间隔、超时、IPv6 和调度周期；`[[targets]]` 定义目标名称、地址、端口和标签。

```toml
[params]
count = 3
interval_millis = 1000
timeout_millis = 2000
enable_ipv6 = true
schedule_seconds = 30

[[targets]]
name = "cloudflare-dns"
address = "1.1.1.1"
port = 53
labels = ["dns", "public"]
```

Supervisor 每 5 秒检查一次配置文件。目标、探测参数、保留策略、Dashboard 时间范围、故障阈值和 `agent_token` 可热加载；监听地址、数据库路径及 Dashboard 凭据修改后需要重启。无效的新配置会被拒绝，服务继续使用上一份有效配置。

### Agent

```toml
supervisor_url = "http://127.0.0.1:8080"
agent_name = "agent-1"
agent_token = ""
poll_interval_seconds = 30
probe_concurrency = 20
max_pending_results = 1000
public_ipv4_url = "https://api-ipv4.ip.sb/ip"
public_ipv6_url = "https://api-ipv6.ip.sb/ip"
```

Agent 使用 Supervisor 下发的 `schedule_seconds` 调度；任务为空、拉取失败或周期无效时使用 `poll_interval_seconds`。上报失败的结果暂存在内存中，最多保留 `max_pending_results` 条，并按每批最多 200 条重试。连续失败时采用退避重试。

公网 IPv4/IPv6 查询结果缓存 15 分钟。查询失败不会中断探测；没有可用结果时，Supervisor 使用请求来源地址记录 Agent IP。

## 认证

Dashboard 使用表单登录，会话有效期为 12 小时。`/api/agents`、`/api/results` 和 `/ws` 也接受相同凭据的 HTTP Basic Auth。

为 Supervisor 和所有 Agent 配置相同的非空 `agent_token` 后，`/api/tasks` 和 `/api/report` 将要求 Bearer Token。公网部署应启用该配置，并通过 HTTPS 反向代理访问。

PowerShell 可生成随机 Token：

```powershell
[Convert]::ToHexString((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
```

## 命令行

```text
supervisor:
  -config        配置文件路径，默认 configs/supervisor.toml
  -format        强制使用 json 或 toml
  -migrate-only  仅迁移 SQLite 结构后退出

agent:
  -config        Agent 配置文件路径
  -format        强制使用 json 或 toml
  -supervisor    覆盖 supervisor_url
  -once          执行一轮拉取、探测和上报后退出
```

## HTTP API

| 方法与路径 | 用途 | 认证 |
| --- | --- | --- |
| `GET /api/tasks` | 获取任务并更新 Agent 心跳 | Agent Token（启用时） |
| `POST /api/report` | 上报单条或一组结果 | Agent Token（启用时） |
| `GET /api/agents` | 查询 Agent 状态 | Dashboard 凭据 |
| `DELETE /api/agents?agent=...` | 删除 Agent 及其全部数据 | Dashboard 凭据 |
| `GET /api/results?range=24h&agent=...` | 查询历史结果 | Dashboard 凭据 |
| `GET /healthz` | 进程存活检查 | 无 |
| `GET /readyz` | 进程及 SQLite 就绪检查 | 无 |

时间范围支持 `m`、`h`、`d`、`w`、`mo`，其中一个月按 30 天计算。

```bash
curl -u admin:change-me \
  'http://127.0.0.1:8080/api/results?range=24h&agent=agent-1'
```

## 数据保留

Supervisor 启动时及每 24 小时执行一次清理：超过 `raw_retention_days` 的原始结果先按 `rollup_interval_minutes` 聚合，再删除原始记录；超过 `retention_days` 的聚合结果会被删除。删除 Agent 会同时删除其心跳、原始结果、聚合结果和序列元数据，且不可恢复。

## Docker

Dockerfile 从 GitHub Release 下载对应架构的预编译包：

```bash
docker build -t pingmon --build-arg VERSION=v0.1.0 .
docker run --rm -p 8080:8080 \
  -v pingmon-data:/opt/pingmon/data pingmon
```

省略 `VERSION` 时使用最新 Release。运行 Agent：

```bash
docker run --rm --entrypoint /opt/pingmon/agent pingmon \
  -config /opt/pingmon/configs/agent.toml \
  -supervisor http://host.docker.internal:8080
```

镜像支持 Linux `amd64`、`386`、`arm64`、`arm/v6`、`arm/v7`、`riscv64` 和 `loong64`。Release 还提供 Windows、macOS 和 FreeBSD 构建，具体架构以发布附件为准。

## 开发

```bash
go test ./...
go build ./cmd/supervisor ./cmd/agent
```

## License

[GNU Affero General Public License v3.0](LICENSE)
