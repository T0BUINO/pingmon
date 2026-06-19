# PingMon

PingMon 是一个 Go 实现的分布式 TCP 探测系统，包含两个独立程序：

- `supervisor`：HTTP 服务端，提供任务接口、结果上报接口和带密码验证的 Dashboard。
- `agent`：CLI 探测端，定期拉取任务，并发执行 TCP 探测，然后上报结果。

## 目录

```text
cmd/supervisor     supervisor 入口
cmd/agent          agent 入口
internal/config    JSON/TOML 配置加载
internal/model     公共数据结构
internal/storage   存储接口、SQLite 存储和 JSONL 文件存储实现
configs            示例配置
```

## 构建

当前默认使用最新版 `modernc.org/sqlite`，请使用 Go 1.25+ 构建。

```bash
go build ./cmd/supervisor
go build ./cmd/agent
```

也可以直接运行：

```bash
go run ./cmd/supervisor -config configs/supervisor.toml
go run ./cmd/agent -config configs/agent.toml
```

## Docker

Dockerfile 会根据目标架构自动下载对应 release 预编译包，默认启动 `supervisor`：

```bash
docker build -t pingmon --build-arg VERSION=v0.1.0 .
docker run --rm -p 8080:8080 -v pingmon-data:/opt/pingmon/data pingmon
```

使用最新 release：

```bash
docker build -t pingmon .
```

运行 agent：

```bash
docker run --rm --entrypoint /opt/pingmon/agent pingmon \
  -config /opt/pingmon/configs/agent.toml \
  -supervisor http://host.docker.internal:8080
```

支持的 Linux 镜像架构与 release artifact 对齐：`amd64`、`386`、`arm64`、`arm/v6`、`arm/v7`、`riscv64`、`loong64`。

## API

获取任务：

```bash
curl http://127.0.0.1:8080/api/tasks
```

上报结果：

```bash
curl -X POST http://127.0.0.1:8080/api/report \
  -H 'Content-Type: application/json' \
  -d '{"agent":"agent-1","target_name":"local-ssh","address":"127.0.0.1","port":22,"checked_at":"2026-06-19T12:00:00Z","success_count":3,"failure_count":0,"average_latency_ms":1.2,"success_rate":1}'
```

Dashboard：

```text
http://127.0.0.1:8080/dashboard
```

默认示例账号密码为 `admin` / `change-me`，请在生产环境修改 `dashboard_user` 和 `dashboard_password`。

## 配置说明

`supervisor` 支持 JSON 和本项目所需字段的简单 TOML：

- `listen`：HTTP 监听地址。
- `storage`：存储后端，默认 `sqlite`，也支持 `file`。
- `sqlite_path`：SQLite 数据库路径。
- `data_file`：使用 `storage = "file"` 时的 JSONL 结果文件路径。
- `dashboard_user` / `dashboard_password`：Dashboard 和 `/api/results` 的 Basic Auth 账号密码。
- `dashboard_ranges`：Dashboard 可选时间范围，例如 `["12h", "24h", "3d", "7d", "14d", "30d", "60d", "180d", "365d"]`。
- `default_range`：Dashboard 默认时间范围，例如 `24h`。
- `retention_days`：数据保留天数，默认 `365`。
- `raw_retention_days`：原始上报数据保留天数，默认 `30`。更早的原始数据会先聚合再删除。
- `rollup_interval_minutes`：聚合粒度，默认 `60`，表示旧数据按 1 小时聚合。
- `failure_threshold`：连续失败次数超过该值时在日志打印告警。
- `task_interval_seconds`：任务建议执行频率。
- `[params]`：探测次数、间隔、超时和 IPv6 开关。
- `[[targets]]`：探测目标列表。

Dashboard landing 按 agent 展示卡片，详情页按 target 展示曲线和日志。图表数据按时间范围查询，支持 `h` 小时、`d` 天、`w` 周、`mo` 月，其中 `mo` 按 30 天估算。超过 `raw_retention_days` 的旧数据会从 SQLite `result_rollups` 聚合表读取，近期数据仍保留原始粒度。页面会连接 `/ws`，agent 上报新结果后通过 WebSocket 自动刷新当前视图。

详情页日志只显示最近的 WARN / ERROR 结果，最多 200 条，避免长期运行后成功上报把页面拉得过长。延迟图使用时间戳横轴，并对每条目标曲线做前端采样，长时间范围也会保持可读。

## Agent 参数

```text
-supervisor  可选，覆盖配置文件中的 supervisor_url
-config      可选，agent JSON/TOML 配置
-format      可选，json 或 toml，默认按扩展名识别
-once        可选，只执行一轮后退出，便于测试
```

agent 配置示例：

```toml
supervisor_url = "http://127.0.0.1:8080"
agent_name = "agent-1"
poll_interval_seconds = 30
public_ipv4_url = "https://api-ipv4.ip.sb/ip"
public_ipv6_url = "https://api-ipv6.ip.sb/ip"
```

`public_ipv4_url` / `public_ipv6_url` 用于 agent 主动获取双栈公网 IP 并随结果上报，适合容器内运行时避免只看到 Docker 内网 IP。默认分别使用 `https://api-ipv4.ip.sb/ip` 和 `https://api-ipv6.ip.sb/ip`；如果只获取到一个地址就上报一个，两个都失败时 supervisor 会继续用 HTTP 连接来源 IP 作为兜底。旧配置 `public_ip_url` 仍然可用，但它只会按当前网络出口返回一个地址。

## License

This project is licensed under the GNU Affero General Public License v3.0.
See the [LICENSE](./LICENSE) file for details.
