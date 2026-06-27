# PingMon

PingMon 是一个轻量的分布式 TCP 可用性监控工具，由两个 Go 程序组成：

- `supervisor` 提供任务、结果上报、SQLite 存储和 Web Dashboard。
- `agent` 定期拉取任务，并发执行 TCP 连接探测并批量上报结果。

支持节点在线状态、实时刷新、历史数据聚合、数据保留策略，以及 Linux、Windows、macOS 和 FreeBSD 多架构构建。

## 快速开始

要求 Go 1.25 或更高版本。

```bash
go run ./cmd/supervisor -config configs/supervisor.toml
```

另开一个终端启动探测端：

```bash
go run ./cmd/agent -config configs/agent.toml
```

访问 <http://127.0.0.1:8080/dashboard>，页面会跳转到登录界面。示例配置的账号和密码是 `admin` / `change-me`，部署前请务必修改它们。登录会话使用随机密钥签名、有效期为 12 小时；右上角可以主动退出。

构建二进制：

```bash
go build ./cmd/supervisor
go build ./cmd/agent
```

## 工作方式

1. agent 携带名称和公网 IP 请求 `GET /api/tasks`，同时刷新节点心跳。
2. agent 按任务参数并发探测目标，然后批量请求 `POST /api/report`。
3. supervisor 将结果写入 SQLite，并通过 WebSocket 通知 Dashboard 刷新。
4. 超过 `raw_retention_days` 的原始结果会先聚合；超过 `retention_days` 的聚合结果会被清理。

## 配置

supervisor 支持 JSON 和 TOML，完整示例见 [`configs/supervisor.toml`](configs/supervisor.toml)：

| 字段 | 说明 | 示例默认值 |
| --- | --- | --- |
| `listen` | HTTP 监听地址 | `:8080` |
| `sqlite_path` | SQLite 文件路径 | `data/pingmon.db` |
| `dashboard_user` / `dashboard_password` | Dashboard 登录和只读 API 的认证信息 | `admin` / `change-me` |
| `dashboard_ranges` | Dashboard 可选时间范围 | `5m` 至 `365d` |
| `default_range` | 默认时间范围 | `24h` |
| `retention_days` | 聚合数据保留天数 | `365` |
| `raw_retention_days` | 原始结果保留天数 | `30` |
| `rollup_interval_minutes` | 历史数据聚合粒度 | `60` |
| `failure_threshold` | 记录连续失败告警的阈值 | `3` |
| `task_interval_seconds` | 未配置任务周期时的默认周期 | `30` |

`[params]` 定义单轮探测次数、探测间隔、超时、IPv6 开关和调度周期；`[[targets]]` 定义探测名称、地址、端口和标签。时间范围支持 `m`、`h`、`d`、`w`、`mo`，其中一个月按 30 天计算。

agent 配置见 [`configs/agent.toml`](configs/agent.toml)：

```toml
supervisor_url = "http://127.0.0.1:8080"
agent_name = "agent-1"
poll_interval_seconds = 30
public_ipv4_url = "https://api-ipv4.ip.sb/ip"
public_ipv6_url = "https://api-ipv6.ip.sb/ip"
```

agent 优先采用 supervisor 下发的 `schedule_seconds`；拉取失败、任务为空或周期无效时，回退到 `poll_interval_seconds`。公网 IP 查询失败不会中断探测，supervisor 会使用 HTTP 来源地址兜底。

### 命令行参数

```text
supervisor:
  -config        JSON/TOML 配置文件，默认 configs/supervisor.toml
  -format        强制指定 json 或 toml
  -migrate-only  仅迁移 SQLite 结构后退出

agent:
  -config        JSON/TOML 配置文件
  -format        强制指定 json 或 toml
  -supervisor    覆盖配置中的 supervisor_url
  -once          仅执行一轮，适合调试
```

## HTTP API

除任务获取和结果上报外，API 需要 HTTP Basic Auth。浏览器访问 Dashboard 使用登录表单，不会弹出 Basic Auth 对话框。

```bash
# 获取任务并刷新心跳
curl 'http://127.0.0.1:8080/api/tasks?agent=agent-1&agent_ip=203.0.113.10'

# 上报单条结果；也可提交结果数组
curl -X POST http://127.0.0.1:8080/api/report \
  -H 'Content-Type: application/json' \
  -d '{"agent":"agent-1","target_name":"local-ssh","address":"127.0.0.1","port":22,"success_count":3,"failure_count":0,"average_latency_ms":1.2,"success_rate":1}'

# 查询节点状态
curl -u admin:change-me http://127.0.0.1:8080/api/agents

# 查询最近 24 小时结果
curl -u admin:change-me 'http://127.0.0.1:8080/api/results?range=24h&agent=agent-1'

# 删除某节点及其全部历史数据
curl -X DELETE -u admin:change-me \
  'http://127.0.0.1:8080/api/agents?agent=agent-1'
```

删除节点会同时移除心跳、原始结果、聚合结果和序列元数据，且不可恢复。

## Docker

Dockerfile 从 GitHub Release 下载与目标架构匹配的预编译包：

```bash
docker build -t pingmon --build-arg VERSION=v0.1.0 .
docker run --rm -p 8080:8080 \
  -v pingmon-data:/opt/pingmon/data pingmon
```

省略 `VERSION` 会下载最新 Release。运行 agent：

```bash
docker run --rm --entrypoint /opt/pingmon/agent pingmon \
  -config /opt/pingmon/configs/agent.toml \
  -supervisor http://host.docker.internal:8080
```

Linux 镜像支持 `amd64`、`386`、`arm64`、`arm/v6`、`arm/v7`、`riscv64` 和 `loong64`。

## 开发

```bash
go test ./...
go vet ./...
```

目录结构：

```text
cmd/agent          agent 入口与探测逻辑
cmd/supervisor     HTTP 服务、登录页和嵌入式 Dashboard 资源
internal/config    JSON/TOML 配置加载
internal/model     公共数据结构
internal/storage   SQLite 存储、迁移和流式查询
configs            示例配置
```

## License

[GNU Affero General Public License v3.0](LICENSE)
