package main

import (
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"flag"
	"html/template"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"
	"pingmon/internal/storage"
)

type server struct {
	cfg   config.Config
	store storage.Store
	tpl   *template.Template
	hub   *websocketHub
}

type dashboardData struct {
	Results       []model.Result
	Agent         string
	Ranges        []string
	SelectedRange string
}

type websocketHub struct {
	mu      sync.Mutex
	clients map[net.Conn]struct{}
}

func main() {
	configPath := flag.String("config", "configs/supervisor.toml", "path to JSON or TOML config")
	format := flag.String("format", "", "config format: json or toml")
	flag.Parse()

	cfg, err := config.Load(*configPath, *format)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	store, err := storage.New(cfg.Storage, cfg.DataFile, cfg.SQLitePath)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	tpl := template.Must(template.New("dashboard").Funcs(template.FuncMap{
		"mul": func(a, b float64) float64 { return a * b },
	}).Parse(dashboardHTML))
	s := &server{cfg: cfg, store: store, tpl: tpl, hub: newWebsocketHub()}
	go s.startRetentionCleaner()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	})
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/report", s.handleReport)
	mux.HandleFunc("/api/results", s.requireDashboardAuth(s.handleResults))
	mux.HandleFunc("/ws", s.requireDashboardAuth(s.handleWebSocket))
	mux.HandleFunc("/dashboard", s.handleDashboard)

	log.Printf("supervisor listening on %s", cfg.Listen)
	log.Fatal(http.ListenAndServe(cfg.Listen, mux))
}

func (s *server) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tasks := make([]model.Task, 0, len(s.cfg.Targets))
	for _, target := range s.cfg.Targets {
		tasks = append(tasks, model.Task{Target: target, Params: s.cfg.Params})
	}
	writeJSON(w, tasks)
}

func (s *server) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	results, err := decodeReportPayload(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(results) == 0 {
		http.Error(w, "empty report payload", http.StatusBadRequest)
		return
	}
	agentIP := remoteIP(r.RemoteAddr)
	for _, result := range results {
		normalizeReport(&result, agentIP)
		if err := s.store.SaveResult(result); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		failures, err := s.store.ConsecutiveFailures(result.TargetName, result.Address, result.Port)
		if err == nil && failures > s.cfg.FailureThreshold {
			log.Printf("[ALERT] target=%s address=%s:%d consecutive_failures=%d threshold=%d",
				result.TargetName, result.Address, result.Port, failures, s.cfg.FailureThreshold)
		}
	}
	s.hub.broadcast("refresh")
	writeJSON(w, map[string]any{"status": "ok", "saved": len(results)})
}

func decodeReportPayload(raw json.RawMessage) ([]model.Result, error) {
	var results []model.Result
	if err := json.Unmarshal(raw, &results); err == nil {
		return results, nil
	}
	var result model.Result
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return []model.Result{result}, nil
}

func normalizeReport(result *model.Result, agentIP string) {
	if result.CheckedAt.IsZero() {
		result.CheckedAt = time.Now()
	}
	if result.AgentIP == "" {
		result.AgentIP = agentIP
	}
	total := result.SuccessCount + result.FailureCount
	if total > 0 {
		result.SuccessRate = float64(result.SuccessCount) / float64(total)
	}
}

func (s *server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if !s.checkDashboardAuth(w, r) {
		return
	}
	selectedRange := s.selectedRange(r)
	results, err := s.store.ResultsSince(time.Now().Add(-rangeDuration(selectedRange)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	agent := r.URL.Query().Get("agent")
	if agent != "" {
		results = filterResultsByAgent(results, agent)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tpl.Execute(w, dashboardData{
		Results:       results,
		Agent:         agent,
		Ranges:        s.cfg.DashboardRanges,
		SelectedRange: selectedRange,
	}); err != nil {
		log.Printf("render dashboard: %v", err)
	}
}

func (s *server) handleResults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	selectedRange := s.selectedRange(r)
	results, err := s.store.ResultsSince(time.Now().Add(-rangeDuration(selectedRange)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if agent := r.URL.Query().Get("agent"); agent != "" {
		results = filterResultsByAgent(results, agent)
	}
	writeJSON(w, results)
}

func (s *server) selectedRange(r *http.Request) string {
	raw := r.URL.Query().Get("range")
	if raw == "" {
		raw = s.cfg.DefaultRange
	}
	for _, allowed := range s.cfg.DashboardRanges {
		if raw == allowed {
			return raw
		}
	}
	if len(s.cfg.DashboardRanges) > 0 {
		return s.cfg.DashboardRanges[0]
	}
	return "24h"
}

func rangeDuration(raw string) time.Duration {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return 24 * time.Hour
	}
	unit := raw[len(raw)-1:]
	valueText := raw[:len(raw)-1]
	multiplier := time.Hour
	if strings.HasSuffix(raw, "mo") {
		unit = "mo"
		valueText = strings.TrimSuffix(raw, "mo")
		multiplier = 30 * 24 * time.Hour
	} else {
		switch unit {
		case "h":
			multiplier = time.Hour
		case "d":
			multiplier = 24 * time.Hour
		case "w":
			multiplier = 7 * 24 * time.Hour
		default:
			return 24 * time.Hour
		}
	}
	n, err := strconv.Atoi(valueText)
	if err != nil || n <= 0 {
		return 24 * time.Hour
	}
	_ = unit
	return time.Duration(n) * multiplier
}

func filterResultsByAgent(results []model.Result, agent string) []model.Result {
	filtered := make([]model.Result, 0, len(results))
	for _, result := range results {
		if result.Agent == agent {
			filtered = append(filtered, result)
		}
	}
	return filtered
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func (s *server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		log.Printf("websocket hijack failed: %v", err)
		return
	}
	accept := websocketAccept(key)
	_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	_, _ = rw.WriteString("Upgrade: websocket\r\n")
	_, _ = rw.WriteString("Connection: Upgrade\r\n")
	_, _ = rw.WriteString("Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return
	}
	s.hub.add(conn)
	_ = writeWebSocketText(conn, "connected")
	go func() {
		defer s.hub.remove(conn)
		buf := make([]byte, 2)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func newWebsocketHub() *websocketHub {
	return &websocketHub{clients: make(map[net.Conn]struct{})}
}

func (h *websocketHub) add(conn net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[conn] = struct{}{}
}

func (h *websocketHub) remove(conn net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, conn)
	_ = conn.Close()
}

func (h *websocketHub) broadcast(message string) {
	h.mu.Lock()
	clients := make([]net.Conn, 0, len(h.clients))
	for conn := range h.clients {
		clients = append(clients, conn)
	}
	h.mu.Unlock()
	for _, conn := range clients {
		if err := writeWebSocketText(conn, message); err != nil {
			h.remove(conn)
		}
	}
}

func writeWebSocketText(conn net.Conn, message string) error {
	payload := []byte(message)
	header := []byte{0x81}
	switch {
	case len(payload) < 126:
		header = append(header, byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		header = append(header, 127, 0, 0, 0, 0, byte(len(payload)>>24), byte(len(payload)>>16), byte(len(payload)>>8), byte(len(payload)))
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func (s *server) requireDashboardAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.checkDashboardAuth(w, r) {
			return
		}
		next(w, r)
	}
}

func (s *server) checkDashboardAuth(w http.ResponseWriter, r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	if ok &&
		subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.DashboardUser)) == 1 &&
		subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.DashboardPassword)) == 1 {
		http.SetCookie(w, &http.Cookie{
			Name:     "pingmon_auth",
			Value:    base64.StdEncoding.EncodeToString([]byte(user + ":" + pass)),
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
		return true
	}
	if cookie, err := r.Cookie("pingmon_auth"); err == nil && s.validDashboardCookie(cookie.Value) {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="PingMon Dashboard"`)
	http.Error(w, "authentication required", http.StatusUnauthorized)
	return false
}

func (s *server) validDashboardCookie(value string) bool {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return false
	}
	user, pass, ok := strings.Cut(string(decoded), ":")
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.DashboardUser)) == 1 &&
		subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.DashboardPassword)) == 1
}

func (s *server) startRetentionCleaner() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	s.cleanOldData()
	for range ticker.C {
		s.cleanOldData()
	}
}

func (s *server) cleanOldData() {
	cutoff := time.Now().AddDate(0, 0, -s.cfg.RetentionDays)
	deleted, err := s.store.DeleteBefore(cutoff)
	if err != nil {
		log.Printf("retention cleanup failed: %v", err)
		return
	}
	if deleted > 0 {
		log.Printf("retention cleanup deleted %d old records", deleted)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

const dashboardHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PingMon Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.9/dist/chart.umd.min.js"></script>
  <style>
    :root { --border: #d8dee8; --muted: #64748b; --ink: #172033; --bg: #f5f7fa; --panel: #ffffff; --accent: #2563eb; }
    * { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 0; color: var(--ink); background: var(--bg); }
    main { max-width: 1280px; margin: 0 auto; padding: 24px; }
    header { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
    h1 { margin: 0; font-size: 26px; line-height: 1.15; letter-spacing: 0; }
    h2 { margin: 0 0 12px; font-size: 17px; }
    a { color: inherit; text-decoration: none; }
    .subtle { display: flex; align-items: center; gap: 8px; color: var(--muted); font-size: 13px; margin-top: 4px; }
    .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 18px; }
    .page-actions { display: flex; align-items: center; gap: 8px; padding: 4px; border: 1px solid #e2e8f0; border-radius: 8px; background: #fff; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; }
    .agent-card { display: block; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 14px; min-height: 220px; transition: border-color .15s, transform .15s, box-shadow .15s; }
    .agent-card:hover { border-color: #94a3b8; transform: translateY(-1px); box-shadow: 0 10px 24px rgba(15, 23, 42, .08); }
    .card-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 10px; }
    .agent-name { font-weight: 700; font-size: 17px; overflow-wrap: anywhere; }
    .status { border-radius: 999px; padding: 3px 8px; font-size: 12px; font-weight: 700; white-space: nowrap; }
    .status.ok { background: #dcfce7; color: #166534; }
    .status.bad { background: #fee2e2; color: #991b1b; }
    .metrics { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 12px 0; }
    .metric { border: 1px solid #e5eaf1; border-radius: 6px; padding: 8px; background: #fbfcfe; min-width: 0; }
    .metric span { display: block; color: var(--muted); font-size: 12px; }
    .metric strong { display: block; margin-top: 3px; font-size: 15px; overflow-wrap: anywhere; }
    .mini-chart { height: 86px; }
    .detail-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
    .chart-wrap { height: 390px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-bottom: 12px; }
    .toolbar label { display: inline-flex; align-items: center; gap: 5px; border: 1px solid var(--border); border-radius: 6px; padding: 4px 8px; background: #fbfcfe; font-size: 13px; line-height: 1.2; }
    .toolbar input[type="checkbox"] { width: 14px; height: 14px; margin: 0; padding: 0; }
    .live-badge { display: inline-flex; align-items: center; gap: 6px; border: 1px solid #bbf7d0; border-radius: 999px; padding: 2px 8px; background: #f0fdf4; color: #166534; font-size: 12px; font-weight: 600; vertical-align: middle; }
    .live-badge::before { content: ""; width: 6px; height: 6px; border-radius: 999px; background: currentColor; }
    .live-badge.reconnecting { border-color: #fed7aa; background: #fff7ed; color: #9a3412; }
    input { height: 34px; border: 1px solid transparent; border-radius: 6px; padding: 0 11px; background: #f8fafc; color: var(--ink); font-size: 14px; line-height: 34px; }
    .range-menu { position: relative; }
    .range-button { min-width: 78px; justify-content: space-between; gap: 10px; }
    .range-button::after { content: ""; width: 0; height: 0; border-left: 4px solid transparent; border-right: 4px solid transparent; border-top: 5px solid #475569; }
    .range-options { position: absolute; top: calc(100% + 6px); left: 0; z-index: 20; display: none; min-width: 96px; padding: 4px; border: 1px solid var(--border); border-radius: 8px; background: #fff; box-shadow: 0 12px 26px rgba(15, 23, 42, .14); }
    .range-menu.open .range-options { display: block; }
    .range-option { display: block; width: 100%; height: 30px; padding: 0 10px; border: 0; background: transparent; text-align: left; border-radius: 6px; }
    .range-option:hover, .range-option.active { background: #eef2f7; }
    button, .button { display: inline-flex; align-items: center; justify-content: center; height: 34px; border: 1px solid transparent; background: #f8fafc; border-radius: 6px; padding: 0 12px; cursor: pointer; font-size: 14px; color: var(--ink); }
    button:hover, .button:hover { background: #eef2f7; border-color: transparent; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { border-bottom: 1px solid #e7ebf1; padding: 9px; text-align: left; }
    th { background: #eef2f7; }
    .ok { color: #137333; font-weight: 600; }
    .bad { color: #b3261e; font-weight: 600; }
    @media (max-width: 760px) {
      main { padding: 16px; }
      header { align-items: flex-start; flex-direction: column; }
      .detail-grid { grid-template-columns: repeat(2, 1fr); }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>{{if .Agent}}{{.Agent}}{{else}}监测节点{{end}}</h1>
        <div class="subtle" id="pageSubtitle">{{if .Agent}}最后上报：--{{else}}分布式 TCP 探测概览{{end}}<span class="live-badge" id="liveState">实时</span></div>
      </div>
      <form class="page-actions" method="get" action="/dashboard">
        {{if .Agent}}<input type="hidden" name="agent" value="{{.Agent}}">{{end}}
        <div class="range-menu" id="rangeMenu">
          <button class="range-button" type="button" id="rangeButton">{{.SelectedRange}}</button>
          <div class="range-options" id="rangeOptions">
            {{range .Ranges}}<button class="range-option {{if eq . $.SelectedRange}}active{{end}}" type="button" data-range="{{.}}">{{.}}</button>{{end}}
          </div>
        </div>
        <button type="button" id="refreshButton">刷新</button>
        {{if .Agent}}<a class="button" href="/dashboard?range={{.SelectedRange}}">返回</a>{{end}}
      </form>
    </header>

    {{if .Agent}}
      <section class="panel">
        <h2>节点信息</h2>
        <div class="detail-grid" id="agentInfo"></div>
      </section>
      <section class="panel">
        <h2>目标延迟</h2>
        <div class="toolbar" id="targetToggles"></div>
        <div class="chart-wrap"><canvas id="latency"></canvas></div>
      </section>
      <section class="panel">
        <h2>上报日志</h2>
        <table>
          <thead>
            <tr><th>时间</th><th>节点 IP</th><th>目标</th><th>地址</th><th>成功率</th><th>平均延迟</th><th>错误</th></tr>
          </thead>
          <tbody>
          {{range .Results}}
            <tr>
              <td class="local-time" data-time="{{.CheckedAt.Format "2006-01-02T15:04:05.999999999Z07:00"}}">{{.CheckedAt.Format "2006-01-02 15:04:05"}}</td>
              <td>{{.AgentIP}}</td>
              <td>{{.TargetName}}</td>
              <td>{{.Address}}:{{.Port}}</td>
              <td class="{{if gt .SuccessRate 0.99}}ok{{else}}bad{{end}}">{{printf "%.1f" (mul .SuccessRate 100)}}%</td>
              <td>{{printf "%.2f" .AverageLatencyMS}} ms</td>
              <td>{{.Error}}</td>
            </tr>
          {{else}}
            <tr><td colspan="7">暂无数据</td></tr>
          {{end}}
          </tbody>
        </table>
      </section>
    {{else}}
      <section class="cards" id="agentCards"></section>
    {{end}}
  </main>
  <script>
    const colors = ['#2563eb', '#16a34a', '#dc2626', '#9333ea', '#d97706', '#0891b2', '#be123c', '#4f46e5'];
    const selectedAgent = '{{.Agent}}';
    let selectedRange = '{{.SelectedRange}}';
    let detailChart = null;
    let miniCharts = [];
    document.querySelectorAll('.local-time').forEach(cell => {
      const date = new Date(cell.dataset.time);
      if (!Number.isNaN(date.getTime())) cell.textContent = date.toLocaleString();
    });
    async function loadResults() {
      const agentParam = selectedAgent ? '&agent=' + encodeURIComponent(selectedAgent) : '';
      const res = await fetch('/api/results?range=' + encodeURIComponent(selectedRange) + agentParam);
      if (!res.ok) throw new Error('结果数据加载失败，状态码：' + res.status);
      return (await res.json()).reverse();
    }
    function targetKey(row) {
      return row.target_name + ' (' + row.address + ':' + row.port + ')';
    }
    function buildDatasets(rows) {
      const labels = [...new Set(rows.map(row => new Date(row.checked_at).toLocaleString()))];
      const grouped = new Map();
      for (const row of rows) {
        const key = targetKey(row);
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key).push({x: new Date(row.checked_at).toLocaleString(), y: row.average_latency_ms});
      }
      const datasets = Array.from(grouped.entries()).map(([label, points], index) => {
        const byTime = new Map(points.map(point => [point.x, point.y]));
        return {
          label,
          data: labels.map(label => byTime.has(label) ? byTime.get(label) : null),
          borderColor: colors[index % colors.length],
          backgroundColor: colors[index % colors.length],
          tension: 0.25,
          pointRadius: 2,
          borderWidth: 2,
          spanGaps: true
        };
      });
      return {labels, datasets};
    }
    function summarizeAgent(rows) {
      const latest = rows[rows.length - 1];
      const totalSuccess = rows.reduce((sum, row) => sum + row.success_count, 0);
      const totalFailure = rows.reduce((sum, row) => sum + row.failure_count, 0);
      const total = totalSuccess + totalFailure;
      const successRate = total ? totalSuccess / total : 0;
      const latencies = rows.filter(row => row.success_count > 0).map(row => row.average_latency_ms);
      const averageLatency = latencies.length ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0;
      const targets = new Set(rows.map(targetKey));
      return {latest, successRate, averageLatency, targetCount: targets.size};
    }
    function metric(label, value) {
      const div = document.createElement('div');
      div.className = 'metric';
      const span = document.createElement('span');
      span.textContent = label;
      const strong = document.createElement('strong');
      strong.textContent = value;
      div.append(span, strong);
      return div;
    }
    function renderLanding(rows) {
      const wrap = document.getElementById('agentCards');
      miniCharts.forEach(chart => chart.destroy());
      miniCharts = [];
      wrap.innerHTML = '';
      const groups = new Map();
      for (const row of rows) {
        if (!groups.has(row.agent)) groups.set(row.agent, []);
        groups.get(row.agent).push(row);
      }
      if (!groups.size) {
        wrap.innerHTML = '<div class="panel">暂无节点上报数据</div>';
        return;
      }
      Array.from(groups.entries()).forEach(([agent, agentRows], index) => {
        const summary = summarizeAgent(agentRows);
        const card = document.createElement('a');
        card.className = 'agent-card';
        card.href = '/dashboard?agent=' + encodeURIComponent(agent) + '&range=' + encodeURIComponent(selectedRange);
        card.innerHTML =
          '<div class="card-head"><div><div class="agent-name"></div><div class="subtle"></div></div><span class="status"></span></div>' +
          '<div class="metrics"></div><div class="mini-chart"><canvas></canvas></div>';
        card.querySelector('.agent-name').textContent = agent;
        card.querySelector('.subtle').textContent = '节点 IP：' + (summary.latest.agent_ip || '未知') + ' · 最后上报：' + new Date(summary.latest.checked_at).toLocaleString();
        const status = card.querySelector('.status');
        status.textContent = summary.successRate >= 0.99 ? '正常' : '异常';
        status.className = 'status ' + (summary.successRate >= 0.99 ? 'ok' : 'bad');
        const metrics = card.querySelector('.metrics');
        metrics.append(metric('目标数', String(summary.targetCount)));
        metrics.append(metric('成功率', (summary.successRate * 100).toFixed(1) + '%'));
        metrics.append(metric('平均延迟', summary.averageLatency.toFixed(1) + ' ms'));
        wrap.appendChild(card);
        const chartData = buildDatasets(agentRows);
        const miniChart = new Chart(card.querySelector('canvas'), {
          type: 'line',
          data: chartData,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            scales: {x: {display: false}, y: {display: false, beginAtZero: true}},
            elements: {point: {radius: 0}},
            plugins: {legend: {display: false}, tooltip: {enabled: false}}
          }
        });
        miniCharts.push(miniChart);
      });
    }
    function renderAgentInfo(rows) {
      const wrap = document.getElementById('agentInfo');
      wrap.innerHTML = '';
      const summary = summarizeAgent(rows);
      const subtitle = document.getElementById('pageSubtitle');
      subtitle.innerHTML = '最后上报：' + new Date(summary.latest.checked_at).toLocaleString() + '<span class="live-badge" id="liveState">实时</span>';
      wrap.append(metric('节点名称', summary.latest.agent || selectedAgent));
      wrap.append(metric('节点 IP', summary.latest.agent_ip || '未知'));
      wrap.append(metric('监测目标', String(summary.targetCount)));
      wrap.append(metric('最后上报', new Date(summary.latest.checked_at).toLocaleString()));
    }
    function renderToggles(chart) {
      const wrap = document.getElementById('targetToggles');
      wrap.innerHTML = '';
      chart.data.datasets.forEach((dataset, index) => {
        const label = document.createElement('label');
        const input = document.createElement('input');
        input.type = 'checkbox'; input.checked = true;
        input.addEventListener('change', () => {
          chart.setDatasetVisibility(index, input.checked);
          chart.update();
        });
        label.append(input, document.createTextNode(dataset.label));
        wrap.appendChild(label);
      });
    }
    async function refreshDashboard() {
      const rows = await loadResults();
      if (!selectedAgent) {
        renderLanding(rows);
        return;
      }
      if (!rows.length) {
        document.getElementById('agentInfo').innerHTML = '<div class="metric"><span>状态</span><strong>暂无数据</strong></div>';
        return;
      }
      renderAgentInfo(rows);
      const chartData = buildDatasets(rows);
      if (detailChart) {
        detailChart.data = chartData;
        detailChart.update('none');
        renderToggles(detailChart);
      } else {
        detailChart = new Chart(document.getElementById('latency'), {
          type: 'line',
          data: chartData,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: {mode: 'nearest', intersect: false},
            scales: {
              x: {title: {display: true, text: '检查时间'}},
              y: {beginAtZero: true, title: {display: true, text: '平均延迟 ms'}}
            },
            plugins: {legend: {display: false}, tooltip: {enabled: true}}
          }
        });
        renderToggles(detailChart);
      }
    }
    function handleRefreshError(err) {
      const targetToggles = document.getElementById('targetToggles');
      const agentCards = document.getElementById('agentCards');
      if (targetToggles) targetToggles.textContent = '加载失败：' + err.message;
      if (agentCards) agentCards.innerHTML = '<div class="panel">加载失败：' + err.message + '</div>';
    }
    document.getElementById('refreshButton').addEventListener('click', () => {
      refreshDashboard().catch(handleRefreshError);
    });
    const rangeMenu = document.getElementById('rangeMenu');
    const rangeButton = document.getElementById('rangeButton');
    rangeButton.addEventListener('click', () => rangeMenu.classList.toggle('open'));
    document.querySelectorAll('.range-option').forEach(option => {
      option.addEventListener('click', () => {
        selectedRange = option.dataset.range;
        rangeButton.textContent = selectedRange;
        document.querySelectorAll('.range-option').forEach(item => item.classList.toggle('active', item === option));
        rangeMenu.classList.remove('open');
        const url = new URL(location.href);
        url.searchParams.set('range', selectedRange);
        history.replaceState(null, '', url);
        refreshDashboard().catch(handleRefreshError);
      });
    });
    document.addEventListener('click', event => {
      if (!rangeMenu.contains(event.target)) rangeMenu.classList.remove('open');
    });
    refreshDashboard().catch(handleRefreshError);
    function connectLiveRefresh() {
      const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
      const ws = new WebSocket(proto + location.host + '/ws');
      const liveState = document.getElementById('liveState');
      ws.onopen = () => {
        liveState.textContent = '实时';
        liveState.classList.remove('reconnecting');
      };
      ws.onmessage = event => {
        if (event.data === 'refresh') refreshDashboard().catch(handleRefreshError);
      };
      ws.onclose = () => {
        liveState.textContent = '重连中';
        liveState.classList.add('reconnecting');
        setTimeout(connectLiveRefresh, 3000);
      };
      ws.onerror = () => ws.close();
    }
    connectLiveRefresh();
  </script>
</body>
</html>`
