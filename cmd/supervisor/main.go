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

const maxProblemLogRows = 200

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
		"mul":      func(a, b float64) float64 { return a * b },
		"severity": resultSeverity,
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
	results, err := s.resultsSince(time.Now().Add(-rangeDuration(selectedRange)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	agent := r.URL.Query().Get("agent")
	if agent != "" {
		results = filterResultsByAgent(results, agent)
	}
	templateResults := results
	if agent != "" {
		templateResults = problemResults(results, maxProblemLogRows)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tpl.Execute(w, dashboardData{
		Results:       templateResults,
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
	results, err := s.resultsSince(time.Now().Add(-rangeDuration(selectedRange)))
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

func (s *server) resultsSince(since time.Time) ([]model.Result, error) {
	rawCutoff := time.Now().AddDate(0, 0, -s.cfg.RawRetentionDays)
	if since.Before(rawCutoff) {
		return s.store.ResultsSinceCompacted(since, rawCutoff)
	}
	return s.store.ResultsSince(since)
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

func problemResults(results []model.Result, limit int) []model.Result {
	filtered := make([]model.Result, 0, minInt(len(results), limit))
	for _, result := range results {
		if !isProblemResult(result) {
			continue
		}
		filtered = append(filtered, result)
		if limit > 0 && len(filtered) >= limit {
			break
		}
	}
	return filtered
}

func isProblemResult(result model.Result) bool {
	return result.FailureCount > 0 || result.SuccessRate < 1 || strings.TrimSpace(result.Error) != ""
}

func resultSeverity(result model.Result) string {
	if result.SuccessCount == 0 || result.SuccessRate == 0 {
		return "ERROR"
	}
	return "WARN"
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	now := time.Now()
	rawCutoff := now.AddDate(0, 0, -s.cfg.RawRetentionDays)
	retentionCutoff := now.AddDate(0, 0, -s.cfg.RetentionDays)
	rollupInterval := time.Duration(s.cfg.RollupIntervalMins) * time.Minute

	rolled, err := s.store.RollupBefore(rawCutoff, rollupInterval)
	if err != nil {
		log.Printf("retention rollup failed: %v", err)
		return
	}

	rawDeleteCutoff := rawCutoff.UTC().Truncate(rollupInterval)
	deletedRaw, err := s.store.DeleteBefore(rawDeleteCutoff)
	if err != nil {
		log.Printf("retention raw cleanup failed: %v", err)
		return
	}
	deletedRollups, err := s.store.DeleteRollupsBefore(retentionCutoff)
	if err != nil {
		log.Printf("retention rollup cleanup failed: %v", err)
		return
	}
	if rolled > 0 || deletedRaw > 0 || deletedRollups > 0 {
		log.Printf("retention cleanup rolled=%d deleted_raw=%d deleted_rollups=%d", rolled, deletedRaw, deletedRollups)
		if err := s.store.Vacuum(); err != nil {
			log.Printf("retention vacuum failed: %v", err)
		}
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
    .cards { display: grid; grid-template-columns: repeat(2, minmax(280px, 1fr)); gap: 14px; }
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
    .chart-tools { justify-content: flex-end; margin-top: -6px; }
    .chart-tools button { width: 34px; padding: 0; font-size: 18px; font-weight: 600; }
    .chart-tools button:disabled { color: #94a3b8; cursor: not-allowed; }
    .chart-wrap { position: relative; height: 390px; width: 100%; touch-action: pan-y; }
    .chart-wrap canvas { cursor: grab; }
    .chart-wrap canvas.dragging { cursor: grabbing; }
    .chart-tooltip { position: fixed; z-index: 1000; max-width: min(560px, calc(100vw - 24px)); max-height: min(520px, calc(100vh - 24px)); overflow: hidden; pointer-events: none; border-radius: 6px; padding: 9px 10px; background: rgba(24, 24, 27, .92); color: #fff; font-size: 13px; line-height: 1.35; box-shadow: 0 16px 40px rgba(15, 23, 42, .24); opacity: 0; transform: translate3d(0, 0, 0); transition: opacity .12s ease; }
    .chart-tooltip-title { margin-bottom: 6px; font-weight: 700; white-space: nowrap; }
    .chart-tooltip-row { display: grid; grid-template-columns: 10px minmax(0, 1fr) auto; align-items: center; gap: 6px; white-space: nowrap; }
    .chart-tooltip-swatch { width: 10px; height: 10px; border-radius: 2px; }
    .chart-tooltip-name { overflow: hidden; text-overflow: ellipsis; }
    .chart-tooltip-value { font-variant-numeric: tabular-nums; }
    .chart-tooltip-more { margin-top: 4px; color: #cbd5e1; font-size: 12px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-bottom: 12px; }
    .toolbar label { display: inline-flex; align-items: center; gap: 5px; border: 1px solid var(--border); border-radius: 6px; padding: 4px 8px; background: #fbfcfe; font-size: 13px; line-height: 1.2; }
    .toolbar input[type="checkbox"] { width: 14px; height: 14px; margin: 0; padding: 0; }
    .live-badge { display: inline-flex; align-items: center; gap: 6px; border: 1px solid #bbf7d0; border-radius: 999px; padding: 2px 8px; background: #f0fdf4; color: #166534; font-size: 12px; font-weight: 600; vertical-align: middle; }
    .live-badge::before { content: ""; width: 6px; height: 6px; border-radius: 999px; background: currentColor; }
    .live-badge.reconnecting { border-color: #fed7aa; background: #fff7ed; color: #9a3412; }
    input { height: 34px; border: 1px solid transparent; border-radius: 6px; padding: 0 11px; background: #f8fafc; color: var(--ink); font-size: 14px; line-height: 34px; }
    .range-menu { position: relative; flex: 0 0 auto; }
    .range-button { min-width: 78px; justify-content: space-between; gap: 10px; }
    .range-button::after { content: ""; width: 0; height: 0; border-left: 4px solid transparent; border-right: 4px solid transparent; border-top: 5px solid #475569; }
    .range-options { position: absolute; top: calc(100% + 6px); left: 0; z-index: 1200; display: none; min-width: 96px; padding: 4px; border: 1px solid var(--border); border-radius: 8px; background: #fff; box-shadow: 0 12px 26px rgba(15, 23, 42, .14); }
    .range-menu.open .range-options { display: block; }
    .range-option { display: block; width: 100%; height: 30px; padding: 0 10px; border: 0; background: transparent; text-align: left; border-radius: 6px; }
    .range-option:hover, .range-option.active { background: #eef2f7; }
    button, .button { display: inline-flex; align-items: center; justify-content: center; height: 34px; border: 1px solid transparent; background: #f8fafc; border-radius: 6px; padding: 0 12px; cursor: pointer; font-size: 14px; color: var(--ink); }
    button:hover, .button:hover { background: #eef2f7; border-color: transparent; }
    .table-scroll { width: 100%; overflow-x: auto; border: 1px solid #e7ebf1; border-radius: 8px; }
    .table-scroll table { min-width: 860px; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { border-bottom: 1px solid #e7ebf1; padding: 9px; text-align: left; }
    th, td { white-space: nowrap; }
    td:last-child { white-space: normal; min-width: 220px; overflow-wrap: anywhere; }
    th { background: #eef2f7; }
    .ok { color: #137333; font-weight: 600; }
    .bad { color: #b3261e; font-weight: 600; }
    .warn { color: #a16207; font-weight: 600; }
    .log-meta { margin: -4px 0 10px; }
    @media (max-width: 760px) {
      main { padding: 16px; }
      header { align-items: flex-start; flex-direction: column; }
      .page-actions { width: 100%; overflow: visible; }
      .cards { grid-template-columns: 1fr; }
      .detail-grid { grid-template-columns: repeat(2, 1fr); }
      .toolbar { flex-wrap: wrap; overflow: visible; padding-bottom: 0; }
      .toolbar label { max-width: 100%; }
      .chart-tools { justify-content: flex-start; }
      .chart-wrap { height: 340px; }
      .chart-tooltip { max-width: min(320px, calc(100vw - 20px)); max-height: min(260px, calc(100vh - 20px)); padding: 7px 8px; font-size: 12px; }
      .chart-tooltip-title { margin-bottom: 4px; }
      .chart-tooltip-row { gap: 5px; }
      .page-actions .range-menu { width: auto; }
      .range-options { left: 0; right: auto; min-width: 100%; width: max-content; max-width: calc(100vw - 32px); }
      .range-option { min-width: 72px; }
      .table-scroll { -webkit-overflow-scrolling: touch; }
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
        <div class="toolbar" id="labelFilters"></div>
        <div class="toolbar" id="targetToggles"></div>
        <div class="toolbar chart-tools" id="chartTools">
          <button type="button" id="zoomInButton" title="放大">+</button>
          <button type="button" id="zoomOutButton" title="缩小">-</button>
          <button type="button" id="zoomResetButton" title="复位">↺</button>
        </div>
        <div class="chart-wrap"><canvas id="latency"></canvas></div>
      </section>
      <section class="panel">
        <h2>告警日志</h2>
        <div class="subtle log-meta" id="logMeta">仅显示最近 {{len .Results}} 条 WARN / ERROR</div>
        <div class="table-scroll">
          <table>
            <thead>
              <tr><th>时间</th><th>级别</th><th>节点 IP</th><th>目标</th><th>地址</th><th>成功率</th><th>平均延迟</th><th>错误</th></tr>
            </thead>
            <tbody id="problemLogBody">
            {{range .Results}}
              <tr>
                <td class="local-time" data-time="{{.CheckedAt.Format "2006-01-02T15:04:05.999999999Z07:00"}}">{{.CheckedAt.Format "2006-01-02 15:04:05"}}</td>
                <td class="{{if eq (severity .) "ERROR"}}bad{{else}}warn{{end}}">{{severity .}}</td>
                <td>{{.AgentIP}}</td>
                <td>{{.TargetName}}</td>
                <td>{{.Address}}:{{.Port}}</td>
                <td class="{{if gt .SuccessRate 0.99}}ok{{else}}bad{{end}}">{{printf "%.1f" (mul .SuccessRate 100)}}%</td>
                <td>{{printf "%.2f" .AverageLatencyMS}} ms</td>
                <td>{{.Error}}</td>
              </tr>
            {{else}}
              <tr><td colspan="8">暂无 WARN / ERROR</td></tr>
            {{end}}
            </tbody>
          </table>
        </div>
      </section>
    {{else}}
      <section class="cards" id="agentCards"></section>
    {{end}}
  </main>
  <script>
    const colors = ['#2563eb', '#16a34a', '#dc2626', '#9333ea', '#d97706', '#0891b2', '#be123c', '#4f46e5'];
    const maxChartPointsPerSeries = 900;
    const maxProblemLogRows = 200;
    const minChartGapMs = 5 * 60 * 1000;
    const selectedAgent = '{{.Agent}}';
    let selectedRange = '{{.SelectedRange}}';
    let detailChart = null;
    let miniCharts = [];
    let selectedLabels = null;
    let currentAgentRows = [];
    let chartFullRange = null;
    let chartViewRange = null;
    let pinchStart = null;
    let panStart = null;
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
    function rowLabels(row) {
      return Array.isArray(row.labels) ? row.labels.filter(label => label) : [];
    }
    function availableLabels(rows) {
      const labels = new Set();
      rows.forEach(row => rowLabels(row).forEach(label => labels.add(label)));
      return Array.from(labels).sort((a, b) => a.localeCompare(b));
    }
    function filterRowsByLabels(rows, labels) {
      if (labels === null) return rows;
      if (!labels.size) return [];
      return rows.filter(row => rowLabels(row).some(label => labels.has(label)));
    }
    function timeValue(row) {
      const ts = new Date(row.checked_at).getTime();
      return Number.isNaN(ts) ? null : ts;
    }
    function compactPoints(points, maxPoints) {
      if (points.length <= maxPoints) return points;
      const stride = Math.ceil(points.length / maxPoints);
      const compacted = [];
      for (let i = 0; i < points.length; i += stride) {
        const bucket = points.slice(i, i + stride);
        compacted.push({
          x: bucket[Math.floor(bucket.length / 2)].x,
          y: bucket.reduce((sum, point) => sum + point.y, 0) / bucket.length
        });
      }
      return compacted;
    }
    function medianInterval(points) {
      if (points.length < 3) return minChartGapMs;
      const intervals = [];
      for (let i = 1; i < points.length; i++) {
        const gap = points[i].x - points[i - 1].x;
        if (gap > 0) intervals.push(gap);
      }
      if (!intervals.length) return minChartGapMs;
      intervals.sort((a, b) => a - b);
      return intervals[Math.floor(intervals.length / 2)];
    }
    function splitLongGaps(points) {
      if (points.length < 2) return points;
      const threshold = Math.max(minChartGapMs, medianInterval(points) * 3);
      const split = [points[0]];
      for (let i = 1; i < points.length; i++) {
        if (points[i].x - points[i - 1].x > threshold) {
          split.push({x: points[i - 1].x + 1, y: null});
          split.push({x: points[i].x - 1, y: null});
        }
        split.push(points[i]);
      }
      return split;
    }
    function formatTimeTick(value) {
      const date = new Date(Number(value));
      if (Number.isNaN(date.getTime())) return '';
      const range = selectedRange.toLowerCase();
      if (range.endsWith('h')) return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
      if (range === '24h') return date.toLocaleString([], {month: '2-digit', day: '2-digit', hour: '2-digit'});
      return date.toLocaleDateString([], {month: '2-digit', day: '2-digit'});
    }
    function buildDatasets(rows) {
      const grouped = new Map();
      for (const row of rows) {
        if (row.success_count <= 0) continue;
        const ts = timeValue(row);
        if (ts === null) continue;
        const key = targetKey(row);
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key).push({x: ts, y: row.average_latency_ms});
      }
      const datasets = Array.from(grouped.entries()).map(([label, points], index) => {
        points.sort((a, b) => a.x - b.x);
        const compacted = splitLongGaps(compactPoints(points, maxChartPointsPerSeries));
        return {
          label,
          data: compacted,
          borderColor: colors[index % colors.length],
          backgroundColor: colors[index % colors.length],
          tension: 0.18,
          cubicInterpolationMode: 'monotone',
          pointRadius: 0,
          pointHoverRadius: 4,
          pointHitRadius: 10,
          borderWidth: 2,
          spanGaps: false
        };
      });
      return {datasets};
    }
    function dataRange(chartData) {
      let min = Infinity;
      let max = -Infinity;
      chartData.datasets.forEach(dataset => {
        dataset.data.forEach(point => {
          if (point.y === null) return;
          min = Math.min(min, point.x);
          max = Math.max(max, point.x);
        });
      });
      return Number.isFinite(min) && Number.isFinite(max) && min < max ? {min, max} : null;
    }
    function clampViewRange(range) {
      if (!chartFullRange || !range) return null;
      const fullSpan = chartFullRange.max - chartFullRange.min;
      let span = Math.min(range.max - range.min, fullSpan);
      const minSpan = Math.max(60 * 1000, fullSpan / 200);
      span = Math.max(span, Math.min(minSpan, fullSpan));
      let min = range.min;
      let max = range.max;
      if (min < chartFullRange.min) {
        min = chartFullRange.min;
        max = min + span;
      }
      if (max > chartFullRange.max) {
        max = chartFullRange.max;
        min = max - span;
      }
      return {min, max};
    }
    function updateZoomButtons() {
      const zoomIn = document.getElementById('zoomInButton');
      const zoomOut = document.getElementById('zoomOutButton');
      const zoomReset = document.getElementById('zoomResetButton');
      if (!zoomIn || !zoomOut || !zoomReset) return;
      const canZoom = Boolean(chartFullRange);
      zoomIn.disabled = !canZoom;
      zoomOut.disabled = !canZoom || chartViewRange === null;
      zoomReset.disabled = !canZoom || chartViewRange === null;
    }
    function applyDetailChartRange(mode) {
      if (!detailChart) return;
      const xScale = detailChart.options.scales.x;
      if (chartViewRange) {
        xScale.min = chartViewRange.min;
        xScale.max = chartViewRange.max;
      } else {
        delete xScale.min;
        delete xScale.max;
      }
      detailChart.update(mode || 'none');
      updateZoomButtons();
    }
    function syncChartRange(chartData) {
      const nextFullRange = dataRange(chartData);
      chartFullRange = nextFullRange;
      chartViewRange = clampViewRange(chartViewRange);
      if (!chartFullRange) chartViewRange = null;
    }
    function zoomDetailChart(factor, center) {
      if (!chartFullRange) return;
      const current = chartViewRange || chartFullRange;
      const currentSpan = current.max - current.min;
      const fullSpan = chartFullRange.max - chartFullRange.min;
      const minSpan = Math.max(60 * 1000, fullSpan / 200);
      const nextSpan = Math.max(Math.min(currentSpan * factor, fullSpan), Math.min(minSpan, fullSpan));
      if (nextSpan >= fullSpan) {
        chartViewRange = null;
        applyDetailChartRange();
        return;
      }
      const pivot = Number.isFinite(center) ? center : (current.min + current.max) / 2;
      const ratio = (pivot - current.min) / currentSpan;
      chartViewRange = clampViewRange({
        min: pivot - nextSpan * ratio,
        max: pivot + nextSpan * (1 - ratio)
      });
      applyDetailChartRange();
    }
    function touchDistance(touches) {
      const dx = touches[0].clientX - touches[1].clientX;
      const dy = touches[0].clientY - touches[1].clientY;
      return Math.hypot(dx, dy);
    }
    function touchCenterX(touches) {
      return (touches[0].clientX + touches[1].clientX) / 2;
    }
    function chartValueAtClientX(chart, clientX) {
      const rect = chart.canvas.getBoundingClientRect();
      const scale = chart.scales.x;
      return scale.getValueForPixel(clientX - rect.left);
    }
    function panChartByPixels(chart, dx) {
      if (!panStart || !chartViewRange) return;
      const width = Math.max(1, chart.chartArea.right - chart.chartArea.left);
      const span = panStart.range.max - panStart.range.min;
      const delta = -dx / width * span;
      chartViewRange = clampViewRange({
        min: panStart.range.min + delta,
        max: panStart.range.max + delta
      });
      applyDetailChartRange();
    }
    function chartTooltip() {
      let tooltip = document.getElementById('chartTooltip');
      if (tooltip) return tooltip;
      tooltip = document.createElement('div');
      tooltip.id = 'chartTooltip';
      tooltip.className = 'chart-tooltip';
      document.body.appendChild(tooltip);
      return tooltip;
    }
    function hideChartTooltip(chart) {
      const tooltip = document.getElementById('chartTooltip');
      if (tooltip) tooltip.style.opacity = '0';
      if (chart && chart.tooltip) {
        chart.tooltip.setActiveElements([], {x: 0, y: 0});
        chart.update('none');
      }
    }
    function externalTooltip(context) {
      const tooltipModel = context.tooltip;
      const tooltip = chartTooltip();
      if (!tooltipModel || tooltipModel.opacity === 0) {
        tooltip.style.opacity = '0';
        return;
      }
      tooltip.innerHTML = '';
      const title = document.createElement('div');
      title.className = 'chart-tooltip-title';
      title.textContent = tooltipModel.dataPoints.length ? new Date(tooltipModel.dataPoints[0].parsed.x).toLocaleString() : '';
      tooltip.appendChild(title);
      const compactTooltip = window.matchMedia('(max-width: 760px), (pointer: coarse)').matches;
      const maxItems = compactTooltip ? 8 : 18;
      tooltipModel.dataPoints.slice(0, maxItems).forEach(item => {
        const row = document.createElement('div');
        row.className = 'chart-tooltip-row';
        const swatch = document.createElement('span');
        swatch.className = 'chart-tooltip-swatch';
        swatch.style.background = item.dataset.borderColor;
        const name = document.createElement('span');
        name.className = 'chart-tooltip-name';
        name.textContent = item.dataset.label;
        const value = document.createElement('span');
        value.className = 'chart-tooltip-value';
        value.textContent = item.parsed.y.toFixed(2) + ' ms';
        row.append(swatch, name, value);
        tooltip.appendChild(row);
      });
      const hiddenCount = tooltipModel.dataPoints.length - maxItems;
      if (hiddenCount > 0) {
        const more = document.createElement('div');
        more.className = 'chart-tooltip-more';
        more.textContent = '还有 ' + hiddenCount + ' 项';
        tooltip.appendChild(more);
      }
      tooltip.style.opacity = '1';
      tooltip.style.left = '0px';
      tooltip.style.top = '0px';
      const rect = context.chart.canvas.getBoundingClientRect();
      const tooltipRect = tooltip.getBoundingClientRect();
      let left = rect.left + tooltipModel.caretX + 14;
      let top = rect.top + tooltipModel.caretY + 14;
      if (left + tooltipRect.width > window.innerWidth - 12) {
        left = rect.left + tooltipModel.caretX - tooltipRect.width - 14;
      }
      if (top + tooltipRect.height > window.innerHeight - 12) {
        top = window.innerHeight - tooltipRect.height - 12;
      }
      tooltip.style.left = Math.max(12, left) + 'px';
      tooltip.style.top = Math.max(12, top) + 'px';
    }
    function attachChartZoomHandlers(chart) {
      const canvas = chart.canvas;
      canvas.addEventListener('touchstart', event => {
        hideChartTooltip(chart);
        if (event.touches.length === 1 && chartViewRange) {
          panStart = {
            x: event.touches[0].clientX,
            y: event.touches[0].clientY,
            range: chartViewRange
          };
          return;
        }
        if (event.touches.length === 2) {
          pinchStart = {
            distance: touchDistance(event.touches),
            range: chartViewRange || chartFullRange,
            center: chartValueAtClientX(chart, touchCenterX(event.touches))
          };
          panStart = null;
        }
      }, {passive: true});
      canvas.addEventListener('touchmove', event => {
        if (panStart && chartViewRange && event.touches.length === 1) {
          const touch = event.touches[0];
          const dx = touch.clientX - panStart.x;
          const dy = touch.clientY - panStart.y;
          if (Math.abs(dx) < 6 || Math.abs(dx) < Math.abs(dy)) return;
          event.preventDefault();
          panChartByPixels(chart, dx);
          return;
        }
        if (!pinchStart || event.touches.length !== 2 || !pinchStart.range) return;
        event.preventDefault();
        const distance = touchDistance(event.touches);
        if (distance <= 0) return;
        const factor = pinchStart.distance / distance;
        const span = pinchStart.range.max - pinchStart.range.min;
        const nextSpan = span * factor;
        const ratio = (pinchStart.center - pinchStart.range.min) / span;
        chartViewRange = clampViewRange({
          min: pinchStart.center - nextSpan * ratio,
          max: pinchStart.center + nextSpan * (1 - ratio)
        });
        applyDetailChartRange();
      }, {passive: false});
      canvas.addEventListener('touchend', event => {
        if (event.touches.length < 2) pinchStart = null;
        if (event.touches.length === 0) panStart = null;
        hideChartTooltip(chart);
      });
      canvas.addEventListener('touchcancel', () => {
        panStart = null;
        pinchStart = null;
        hideChartTooltip(chart);
      });
      canvas.addEventListener('wheel', event => {
        if (!event.ctrlKey && !event.metaKey) return;
        hideChartTooltip(chart);
        event.preventDefault();
        zoomDetailChart(event.deltaY < 0 ? 0.75 : 1.35, chartValueAtClientX(chart, event.clientX));
      }, {passive: false});
      canvas.addEventListener('mousedown', event => {
        if (event.button !== 0 || !chartViewRange) return;
        event.preventDefault();
        panStart = {
          x: event.clientX,
          y: event.clientY,
          range: chartViewRange
        };
        canvas.classList.add('dragging');
      });
      window.addEventListener('mousemove', event => {
        if (!panStart || !chartViewRange || event.buttons !== 1) return;
        event.preventDefault();
        panChartByPixels(chart, event.clientX - panStart.x);
      });
      window.addEventListener('mouseup', () => {
        panStart = null;
        canvas.classList.remove('dragging');
      });
      window.addEventListener('blur', () => {
        panStart = null;
        pinchStart = null;
        canvas.classList.remove('dragging');
        hideChartTooltip(chart);
      });
      window.addEventListener('scroll', () => hideChartTooltip(chart), {passive: true});
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
            interaction: {mode: 'x', axis: 'x', intersect: false},
            scales: {x: {type: 'linear', display: false}, y: {display: false, beginAtZero: true}},
            elements: {point: {radius: 0}},
            plugins: {
              legend: {display: false},
              tooltip: {
                enabled: false,
                mode: 'x',
                intersect: false,
                external: externalTooltip,
                itemSort: (a, b) => b.parsed.y - a.parsed.y || a.dataset.label.localeCompare(b.dataset.label),
              }
            }
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
    function problemSeverity(row) {
      return row.success_count === 0 || row.success_rate === 0 ? 'ERROR' : 'WARN';
    }
    function isProblemRow(row) {
      return row.failure_count > 0 || row.success_rate < 1 || Boolean(row.error);
    }
    function renderProblemLog(rows) {
      const tbody = document.getElementById('problemLogBody');
      const meta = document.getElementById('logMeta');
      if (!tbody) return;
      const problems = rows.filter(isProblemRow).slice().reverse().slice(0, maxProblemLogRows);
      tbody.innerHTML = '';
      if (meta) meta.textContent = '仅显示最近 ' + problems.length + ' 条 WARN / ERROR，最多 ' + maxProblemLogRows + ' 条';
      if (!problems.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 8;
        td.textContent = '暂无 WARN / ERROR';
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }
      for (const row of problems) {
        const tr = document.createElement('tr');
        const cells = [
          new Date(row.checked_at).toLocaleString(),
          problemSeverity(row),
          row.agent_ip || '未知',
          row.target_name,
          row.address + ':' + row.port,
          (row.success_rate * 100).toFixed(1) + '%',
          row.success_count > 0 ? row.average_latency_ms.toFixed(2) + ' ms' : '--',
          row.error || ''
        ];
        cells.forEach((value, index) => {
          const td = document.createElement('td');
          td.textContent = value;
          if (index === 1) td.className = value === 'ERROR' ? 'bad' : 'warn';
          if (index === 5) td.className = row.success_rate > 0.99 ? 'ok' : 'bad';
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      }
    }
    function renderToggles(chart) {
      const wrap = document.getElementById('targetToggles');
      wrap.innerHTML = '';
      if (!chart.data.datasets.length) {
        wrap.textContent = '当前 label 下暂无可绘制目标';
        return;
      }
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
    function updateDetailChart(rows) {
      const chartData = buildDatasets(filterRowsByLabels(rows, selectedLabels));
      syncChartRange(chartData);
      if (detailChart) {
        detailChart.data = chartData;
        applyDetailChartRange('none');
        renderToggles(detailChart);
        return;
      }
      detailChart = new Chart(document.getElementById('latency'), {
        type: 'line',
        data: chartData,
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          interaction: {mode: 'x', axis: 'x', intersect: false},
          scales: {
            x: {
              type: 'linear',
              title: {display: true, text: '检查时间'},
              ticks: {maxTicksLimit: window.innerWidth <= 760 ? 4 : 8, callback: value => formatTimeTick(value)}
            },
            y: {beginAtZero: true, title: {display: true, text: '平均延迟 ms'}}
          },
          plugins: {
            legend: {display: false},
            tooltip: {
              enabled: false,
              mode: 'x',
              intersect: false,
              external: externalTooltip,
              itemSort: (a, b) => b.parsed.y - a.parsed.y || a.dataset.label.localeCompare(b.dataset.label),
            }
          }
        }
      });
      attachChartZoomHandlers(detailChart);
      applyDetailChartRange('none');
      renderToggles(detailChart);
    }
    function renderLabelFilters(rows) {
      const wrap = document.getElementById('labelFilters');
      if (!wrap) return;
      const labels = availableLabels(rows);
      wrap.innerHTML = '';
      if (!labels.length) {
        wrap.textContent = '暂无 label';
        selectedLabels = null;
        return;
      }
      const valid = new Set(labels);
      if (selectedLabels === null) {
        selectedLabels = new Set(labels);
      } else {
        selectedLabels = new Set(Array.from(selectedLabels).filter(label => valid.has(label)));
      }
      labels.forEach(labelText => {
        const label = document.createElement('label');
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.checked = selectedLabels.has(labelText);
        input.addEventListener('change', () => {
          if (input.checked) {
            selectedLabels.add(labelText);
          } else {
            selectedLabels.delete(labelText);
          }
          updateDetailChart(currentAgentRows);
        });
        label.append(input, document.createTextNode('label: ' + labelText));
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
        renderProblemLog(rows);
        return;
      }
      currentAgentRows = rows;
      renderAgentInfo(rows);
      renderProblemLog(rows);
      renderLabelFilters(rows);
      updateDetailChart(rows);
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
    const zoomInButton = document.getElementById('zoomInButton');
    const zoomOutButton = document.getElementById('zoomOutButton');
    const zoomResetButton = document.getElementById('zoomResetButton');
    if (zoomInButton) zoomInButton.addEventListener('click', () => zoomDetailChart(0.65));
    if (zoomOutButton) zoomOutButton.addEventListener('click', () => zoomDetailChart(1.55));
    if (zoomResetButton) zoomResetButton.addEventListener('click', () => {
      chartViewRange = null;
      applyDetailChartRange();
    });
    updateZoomButtons();
    const rangeMenu = document.getElementById('rangeMenu');
    const rangeButton = document.getElementById('rangeButton');
    rangeButton.addEventListener('click', () => rangeMenu.classList.toggle('open'));
    document.querySelectorAll('.range-option').forEach(option => {
      option.addEventListener('click', () => {
        selectedRange = option.dataset.range;
        rangeButton.textContent = selectedRange;
        document.querySelectorAll('.range-option').forEach(item => item.classList.toggle('active', item === option));
        rangeMenu.classList.remove('open');
        hideChartTooltip(detailChart);
        chartViewRange = null;
        const url = new URL(location.href);
        url.searchParams.set('range', selectedRange);
        history.replaceState(null, '', url);
        refreshDashboard().catch(handleRefreshError);
      });
    });
    document.addEventListener('click', event => {
      if (!rangeMenu.contains(event.target)) rangeMenu.classList.remove('open');
      const chartTooltip = document.getElementById('chartTooltip');
      if (chartTooltip && !event.target.closest('canvas')) hideChartTooltip(detailChart);
    });
    document.addEventListener('touchstart', event => {
      if (!rangeMenu.contains(event.target)) rangeMenu.classList.remove('open');
      if (!event.target.closest('canvas')) hideChartTooltip(detailChart);
    }, {passive: true});
    window.addEventListener('resize', () => {
      rangeMenu.classList.remove('open');
      hideChartTooltip(detailChart);
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
