package main

import (
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
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
	cfg          config.Config
	store        storage.Store
	tpl          *template.Template
	hub          *websocketHub
	resultsCache *resultsCache
	dashCache    *dashboardResultCache
	dashJobs     chan dashboardCacheKey
}

type dashboardData struct {
	Results             []model.Result
	Agent               string
	Ranges              []string
	SelectedRange       string
	CustomRange         bool
	OfflineAfterSeconds int
}

type websocketHub struct {
	mu      sync.Mutex
	clients map[net.Conn]struct{}
}

type websocketEvent struct {
	Type    string         `json:"type"`
	Results []model.Result `json:"results,omitempty"`
}

type agentStatusView struct {
	Agent               string    `json:"agent"`
	AgentIP             string    `json:"agent_ip,omitempty"`
	FirstSeenAt         time.Time `json:"first_seen_at"`
	LastSeenAt          time.Time `json:"last_seen_at"`
	OfflineAfterSeconds int       `json:"offline_after_seconds"`
	Status              string    `json:"status"`
}

const maxProblemLogRows = 200
const resultsCacheTTL = 2 * time.Second

func main() {
	configPath := flag.String("config", "configs/supervisor.toml", "path to JSON or TOML config")
	format := flag.String("format", "", "config format: json or toml")
	migrateOnly := flag.Bool("migrate-only", false, "migrate SQLite storage and exit")
	flag.Parse()

	cfg, err := config.Load(*configPath, *format)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if *migrateOnly {
		migrated, err := storage.MigrateSQLite(cfg.SQLitePath)
		if err != nil {
			log.Fatalf("migrate sqlite: %v", err)
		}
		if migrated {
			log.Printf("sqlite migration completed: %s", cfg.SQLitePath)
		} else {
			log.Printf("sqlite schema is already current: %s", cfg.SQLitePath)
		}
		return
	}
	store, err := storage.New(cfg.SQLitePath)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	tpl := template.Must(template.New("dashboard").Funcs(template.FuncMap{
		"mul":      func(a, b float64) float64 { return a * b },
		"severity": resultSeverity,
	}).Parse(dashboardHTML))
	s := &server{
		cfg:          cfg,
		store:        store,
		tpl:          tpl,
		hub:          newWebsocketHub(),
		resultsCache: newResultsCache(resultsCacheTTL),
		dashCache:    newDashboardResultCache(cfg.SQLitePath),
		dashJobs:     make(chan dashboardCacheKey, 64),
	}
	s.dashCache.clearIncompatible()
	go s.startRetentionCleaner()
	go s.runDashboardCacheBuilder()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	})
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/report", s.handleReport)
	mux.HandleFunc("/api/agents", s.requireDashboardAuth(s.handleAgents))
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
	agent, agentIP := agentIdentityFromRequest(r)
	if agent != "" {
		if agentIP == "" {
			agentIP = remoteIP(r.RemoteAddr)
		}
		if err := s.store.SaveAgentHeartbeat(agent, agentIP, time.Now()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
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
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&raw); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			http.Error(w, "invalid JSON payload", http.StatusBadRequest)
			return
		}
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
	seenAt := time.Now()
	validatedResults := make([]model.Result, 0, len(results))
	for _, result := range results {
		if err := validateReportResult(result); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		normalizeReport(&result, agentIP)
		validatedResults = append(validatedResults, result)
	}
	saved := make([]model.Result, 0, len(validatedResults))
	for _, result := range validatedResults {
		if err := s.store.SaveResult(result); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := s.store.SaveAgentHeartbeat(result.Agent, result.AgentIP, seenAt); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		saved = append(saved, result)
		failures, err := s.store.ConsecutiveFailures(result.TargetName, result.Address, result.Port)
		if err == nil && failures > s.cfg.FailureThreshold {
			log.Printf("[ALERT] target=%s address=%s:%d consecutive_failures=%d threshold=%d",
				result.TargetName, result.Address, result.Port, failures, s.cfg.FailureThreshold)
		}
	}
	if len(saved) > 0 {
		s.dashCache.appendDelta(saved)
	}
	s.resultsCache.clear()
	s.hub.broadcastJSON(websocketEvent{Type: "results", Results: saved})
	writeJSON(w, map[string]any{"status": "ok", "saved": len(results)})
}

func agentIdentityFromRequest(r *http.Request) (string, string) {
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	agentIP := strings.TrimSpace(r.URL.Query().Get("agent_ip"))
	if agent == "" {
		agent = strings.TrimSpace(r.Header.Get("X-Pingmon-Agent"))
	}
	if agentIP == "" {
		agentIP = strings.TrimSpace(r.Header.Get("X-Pingmon-Agent-IP"))
	}
	if (agent == "" || agentIP == "") && r.Body != nil {
		var payload struct {
			Agent   string `json:"agent"`
			AgentIP string `json:"agent_ip"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			if agent == "" {
				agent = strings.TrimSpace(payload.Agent)
			}
			if agentIP == "" {
				agentIP = strings.TrimSpace(payload.AgentIP)
			}
		}
	}
	return agent, agentIP
}

func decodeReportPayload(raw json.RawMessage) ([]model.Result, error) {
	if strings.TrimSpace(string(raw)) == "null" {
		return nil, fmt.Errorf("report payload is required")
	}
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

func validateReportResult(result model.Result) error {
	if strings.TrimSpace(result.Agent) == "" {
		return fmt.Errorf("agent is required")
	}
	if strings.TrimSpace(result.TargetName) == "" {
		return fmt.Errorf("target_name is required")
	}
	if strings.TrimSpace(result.Address) == "" {
		return fmt.Errorf("address is required")
	}
	if result.Port < 1 || result.Port > 65535 {
		return fmt.Errorf("invalid port")
	}
	if result.SuccessCount < 0 {
		return fmt.Errorf("success_count must be non-negative")
	}
	if result.FailureCount < 0 {
		return fmt.Errorf("failure_count must be non-negative")
	}
	if result.SuccessCount+result.FailureCount <= 0 {
		return fmt.Errorf("success_count and failure_count must include at least one sample")
	}
	if result.AverageLatencyMS < 0 {
		return fmt.Errorf("average_latency_ms must be non-negative")
	}
	if result.SuccessRate < 0 || result.SuccessRate > 1 {
		return fmt.Errorf("success_rate must be between 0 and 1")
	}
	return nil
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
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, max-age=10")
	if err := s.tpl.Execute(w, dashboardData{
		Results:             nil,
		Agent:               agent,
		Ranges:              s.cfg.DashboardRanges,
		SelectedRange:       selectedRange,
		CustomRange:         !rangeInList(selectedRange, s.cfg.DashboardRanges),
		OfflineAfterSeconds: s.agentOfflineAfterSeconds(),
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
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	if r.URL.Query().Get("dashboard") == "1" {
		s.writeDashboardResults(w, selectedRange, agent)
		return
	}
	cacheKey := resultsCacheKey{selectedRange: selectedRange, agent: agent}
	if data, ok := s.resultsCache.get(cacheKey); ok {
		writeJSONBytes(w, data)
		return
	}
	results, err := s.resultsSince(time.Now().Add(-rangeDuration(selectedRange)), agent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(results)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.resultsCache.set(cacheKey, data)
	writeJSONBytes(w, data)
}

func (s *server) handleAgents(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		s.handleDeleteAgent(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	statuses, err := s.store.ListAgentStatuses()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	offlineAfter := s.agentOfflineAfterSeconds()
	now := time.Now()
	out := make([]agentStatusView, 0, len(statuses))
	for _, status := range statuses {
		state := "online"
		if now.Sub(status.LastSeenAt) > time.Duration(offlineAfter)*time.Second {
			state = "offline"
		}
		out = append(out, agentStatusView{
			Agent:               status.Agent,
			AgentIP:             status.AgentIP,
			FirstSeenAt:         status.FirstSeenAt,
			LastSeenAt:          status.LastSeenAt,
			OfflineAfterSeconds: offlineAfter,
			Status:              state,
		})
	}
	writeJSON(w, out)
}

func (s *server) handleDeleteAgent(w http.ResponseWriter, r *http.Request) {
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	if agent == "" {
		http.Error(w, "agent is required", http.StatusBadRequest)
		return
	}
	if err := s.store.DeleteAgent(agent); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.resultsCache.clear()
	s.dashCache.clear()
	if s.hub != nil {
		s.hub.broadcast("refresh")
	}
	writeJSON(w, map[string]any{"status": "ok", "agent": agent})
}

func (s *server) agentOfflineAfterSeconds() int {
	seconds := s.cfg.Params.ScheduleSeconds
	if seconds <= 0 {
		seconds = s.cfg.TaskIntervalSeconds
	}
	if seconds <= 0 {
		seconds = 30
	}
	return seconds * 3
}

func (s *server) selectedRange(r *http.Request) string {
	raw := r.URL.Query().Get("range")
	if raw == "" {
		if cookie, err := r.Cookie("pingmon_range"); err == nil {
			raw = cookie.Value
		}
	}
	if raw == "" {
		raw = s.cfg.DefaultRange
	}
	for _, allowed := range s.cfg.DashboardRanges {
		if raw == allowed {
			return raw
		}
	}
	if _, ok := parseRangeDuration(raw); ok {
		return strings.TrimSpace(strings.ToLower(raw))
	}
	if len(s.cfg.DashboardRanges) > 0 {
		return s.cfg.DashboardRanges[0]
	}
	return "24h"
}

func (s *server) resultsSince(since time.Time, agent string) ([]model.Result, error) {
	rawCutoff := time.Now().AddDate(0, 0, -s.cfg.RawRetentionDays)
	if since.Before(rawCutoff) {
		if agent != "" {
			return s.store.ResultsSinceCompactedForAgent(since, rawCutoff, agent)
		}
		return s.store.ResultsSinceCompacted(since, rawCutoff)
	}
	if agent != "" {
		return s.store.ResultsSinceForAgent(since, agent)
	}
	return s.store.ResultsSince(since)
}

func (s *server) writeDashboardResults(w http.ResponseWriter, selectedRange, agent string) {
	key := dashboardCacheKey{Agent: agent}
	since := time.Now().Add(-rangeDuration(selectedRange))
	w.Header().Set("Cache-Control", "private, max-age=5")
	if stale, err := s.dashCache.writeIfReady(w, key, since); err == nil {
		if stale {
			s.enqueueDashboardCacheBuild(key)
		}
		return
	}
	s.enqueueDashboardCacheBuild(key)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "2")
	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte("[]")); err != nil {
		log.Printf("write dashboard pending response: %v", err)
	}
}

func (s *server) enqueueDashboardCacheBuild(key dashboardCacheKey) {
	if s.dashJobs == nil {
		return
	}
	if !s.dashCache.markPending(key) {
		return
	}
	select {
	case s.dashJobs <- key:
	default:
		s.dashCache.unmarkPending(key)
		log.Printf("dashboard cache build queue full agent=%q", key.Agent)
	}
}

func (s *server) runDashboardCacheBuilder() {
	for key := range s.dashJobs {
		func() {
			defer s.dashCache.unmarkPending(key)
			since := time.Now().Add(-dashboardMaxCacheRange)
			if err := s.dashCache.refresh(key, since, func(fn func(model.Result) error) error {
				return s.streamResultsSince(since, key.Agent, fn)
			}); err != nil {
				log.Printf("dashboard cache build agent=%q: %v", key.Agent, err)
			}
		}()
		time.Sleep(250 * time.Millisecond)
	}
}

func (s *server) streamResultsSince(since time.Time, agent string, fn func(model.Result) error) error {
	rawCutoff := time.Now().AddDate(0, 0, -s.cfg.RawRetentionDays)
	if since.Before(rawCutoff) {
		return s.store.StreamResultsSinceCompacted(since, rawCutoff, agent, fn)
	}
	return s.store.StreamResultsSince(since, agent, fn)
}

func rangeDuration(raw string) time.Duration {
	if duration, ok := parseRangeDuration(raw); ok {
		return duration
	}
	return 24 * time.Hour
}

func parseRangeDuration(raw string) (time.Duration, bool) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return 0, false
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
		case "m":
			multiplier = time.Minute
		case "h":
			multiplier = time.Hour
		case "d":
			multiplier = 24 * time.Hour
		case "w":
			multiplier = 7 * 24 * time.Hour
		default:
			return 0, false
		}
	}
	n, err := strconv.Atoi(valueText)
	if err != nil || n <= 0 {
		return 0, false
	}
	_ = unit
	return time.Duration(n) * multiplier, true
}

func rangeInList(raw string, ranges []string) bool {
	for _, allowed := range ranges {
		if raw == allowed {
			return true
		}
	}
	return false
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

func (h *websocketHub) broadcastJSON(event websocketEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("websocket marshal failed: %v", err)
		h.broadcast("refresh")
		return
	}
	h.broadcast(string(payload))
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
		s.resultsCache.clear()
		s.dashCache.clear()
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

func writeJSONBytes(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(data); err != nil {
		log.Printf("write json: %v", err)
	}
}

type resultsCacheKey struct {
	selectedRange string
	agent         string
}

type resultsCacheEntry struct {
	data      []byte
	expiresAt time.Time
}

type resultsCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[resultsCacheKey]resultsCacheEntry
}

func newResultsCache(ttl time.Duration) *resultsCache {
	return &resultsCache{
		ttl:     ttl,
		entries: make(map[resultsCacheKey]resultsCacheEntry),
	}
}

func (c *resultsCache) get(key resultsCacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[key]
	if !ok || now.After(entry.expiresAt) {
		c.mu.RUnlock()
		if ok {
			c.mu.Lock()
			if current, exists := c.entries[key]; exists && now.After(current.expiresAt) {
				delete(c.entries, key)
			}
			c.mu.Unlock()
		}
		return nil, false
	}
	c.mu.RUnlock()
	return entry.data, true
}

func (c *resultsCache) set(key resultsCacheKey, data []byte) {
	if c == nil || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	c.entries[key] = resultsCacheEntry{
		data:      append([]byte(nil), data...),
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

func (c *resultsCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = make(map[resultsCacheKey]resultsCacheEntry)
	c.mu.Unlock()
}

const dashboardHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PingMon Dashboard</title>
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
    .page-actions { display: flex; flex: 0 0 auto; flex-wrap: wrap; align-items: center; justify-content: flex-end; gap: 8px; margin-left: auto; }
    .cards { display: grid; grid-template-columns: repeat(2, minmax(280px, 1fr)); gap: 14px; }
    .agent-card { display: block; background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 14px; min-height: 220px; content-visibility: auto; contain-intrinsic-size: 360px 220px; contain: layout paint; cursor: pointer; transition: border-color .15s, transform .15s, box-shadow .15s; }
    .agent-card:hover { border-color: #94a3b8; transform: translateY(-1px); box-shadow: 0 10px 24px rgba(15, 23, 42, .08); }
    .card-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 10px; }
    .agent-name { font-weight: 700; font-size: 17px; overflow-wrap: anywhere; }
    .status { border: 1px solid transparent; border-radius: 999px; padding: 2px 8px; font-size: 12px; font-weight: 700; white-space: nowrap; }
    .status.ok { border-color: #86efac; background: #dcfce7; color: #166534; }
    .status.bad { border-color: #fca5a5; background: #fee2e2; color: #991b1b; }
    .status.offline { border-color: #cbd5e1; background: #e5e7eb; color: #374151; }
    .status.idle { border-color: #7dd3fc; background: #e0f2fe; color: #075985; }
    .subtle .status { display: inline-flex; align-items: center; padding: 2px 8px; }
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
    .chart-surface { width: 100%; min-width: 0; overflow: hidden; }
    .chart-wrap .chart-surface { position: absolute; inset: 0; height: 100%; }
    .mini-chart.chart-surface { height: 86px; }
    .chart-surface svg { display: block; width: 100%; height: 100%; cursor: grab; overflow: hidden; }
    .chart-surface svg * { pointer-events: none; }
    .chart-surface.dragging svg { cursor: grabbing; }
    .chart-hover-line { position: absolute; top: 0; bottom: 0; left: 0; width: 1px; background: repeating-linear-gradient(to bottom, #94a3b8 0 4px, transparent 4px 8px); pointer-events: none; opacity: 0; transform: translate3d(0, 0, 0); }
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
    .range-custom { display: flex; flex-direction: column; gap: 6px; margin-top: 4px; padding-top: 6px; border-top: 1px solid var(--border); }
    .range-custom input { width: 100%; height: 30px; border: 1px solid var(--border); border-radius: 6px; padding: 0 8px; font: inherit; }
    .range-custom button { width: 100%; height: 30px; padding: 0 8px; }
    .range-error { display: none; margin-top: 4px; padding: 0 2px; color: var(--bad); font-size: 12px; white-space: nowrap; }
    .range-menu.invalid .range-error { display: block; }
    button, .button { display: inline-flex; align-items: center; justify-content: center; height: 34px; border: 1px solid #e5eaf1; background: #fbfcfe; border-radius: 6px; padding: 0 12px; cursor: pointer; font: 500 14px/1 system-ui, -apple-system, Segoe UI, sans-serif; color: var(--ink); }
    button:hover, .button:hover { background: #f1f5f9; border-color: #cbd5e1; }
    button:focus-visible, .button:focus-visible { outline: 2px solid rgba(37, 99, 235, .28); outline-offset: 2px; }
    [hidden] { display: none !important; }
    button.danger { border-color: #fecaca; background: #fef2f2; color: #991b1b; }
    button.danger:hover { border-color: #fca5a5; background: #fee2e2; }
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
      .page-actions { width: 100%; justify-content: flex-start; margin-left: 0; overflow: visible; }
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
        <div class="subtle" id="pageSubtitle">{{if .Agent}}最后在线：--{{else}}分布式 TCP 探测概览{{end}}<span class="live-badge" id="liveState">实时</span></div>
      </div>
      <form class="page-actions" method="get" action="/dashboard">
        {{if .Agent}}<input type="hidden" name="agent" value="{{.Agent}}">{{end}}
        <div class="range-menu" id="rangeMenu">
          <button class="range-button" type="button" id="rangeButton">{{.SelectedRange}}</button>
          <div class="range-options" id="rangeOptions">
            {{range .Ranges}}<button class="range-option {{if eq . $.SelectedRange}}active{{end}}" type="button" data-range="{{.}}">{{.}}</button>{{end}}
            <div class="range-custom" id="rangeCustomForm">
              <input id="rangeCustomInput" type="text" value="{{if .CustomRange}}{{.SelectedRange}}{{end}}" placeholder="45m" aria-label="自定义范围">
              <button type="button" id="rangeCustomApply">应用</button>
            </div>
            <div class="range-error" id="rangeError">格式：数字 + m/h/d/w/mo</div>
          </div>
        </div>
        <button type="button" id="refreshButton">刷新</button>
        {{if .Agent}}<button class="danger" type="button" id="deleteAgentButton" hidden>删除结果</button>{{end}}
        {{if .Agent}}<a class="button" id="backButton" href="/dashboard?range={{.SelectedRange}}">返回</a>{{end}}
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
        <div class="chart-wrap"><div class="chart-surface" id="latency"></div></div>
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
    const maxProblemLogRows = 200;
    const minChartGapMs = 5 * 60 * 1000;
    const selectedAgent = '{{.Agent}}';
    const defaultOfflineAfterSeconds = {{.OfflineAfterSeconds}};
    let selectedRange = '{{.SelectedRange}}';
    let detailChart = null;
    let miniCharts = new Set();
    let miniChartObserver = null;
    let selectedLabels = null;
    let currentRows = [];
    let currentAgents = [];
    let currentAgentRows = [];
    let chartFullRange = null;
    let chartViewRange = null;
    let targetVisibility = new Map();
    let renderDashboardTimer = null;
    let pendingDashboardRows = null;
    let pinchStart = null;
    let panStart = null;
    document.querySelectorAll('.local-time').forEach(cell => {
      const date = new Date(cell.dataset.time);
      if (!Number.isNaN(date.getTime())) cell.textContent = date.toLocaleString();
    });
    async function loadResults() {
      const agentParam = selectedAgent ? '&agent=' + encodeURIComponent(selectedAgent) : '';
      const res = await fetch('/api/results?dashboard=1&range=' + encodeURIComponent(selectedRange) + agentParam);
      if (res.status === 202) {
        window.setTimeout(() => refreshDashboard().catch(handleRefreshError), 2000);
        return currentRows;
      }
      if (!res.ok) throw new Error('结果数据加载失败，状态码：' + res.status);
      return normalizeResultRows(await res.json()).reverse();
    }
    async function loadAgents() {
      const res = await fetch('/api/agents');
      if (!res.ok) throw new Error('节点状态加载失败，状态码：' + res.status);
      return await res.json();
    }
    async function deleteAgent(agent) {
      if (!agent) return;
      if (!confirm('将删除 ' + agent + ' 的所有历史结果和离线记录，不能撤销。')) return;
      const res = await fetch('/api/agents?agent=' + encodeURIComponent(agent), {method: 'DELETE'});
      if (!res.ok) throw new Error('删除结果失败，状态码：' + res.status);
      currentAgents = currentAgents.filter(item => item.agent !== agent);
      currentRows = currentRows.filter(row => row.agent !== agent);
      if (selectedAgent === agent) {
        location.href = '/dashboard?range=' + encodeURIComponent(selectedRange);
        return;
      }
      renderDashboardRows(currentRows);
    }
    function parseRangeMillis(raw) {
      const value = String(raw || '24h').trim().toLowerCase();
      const match = value.match(/^(\d+)(m|h|d|w|mo)$/);
      if (!match) return 24 * 60 * 60 * 1000;
      const amount = Number(match[1]);
      const multipliers = {
        m: 60 * 1000,
        h: 60 * 60 * 1000,
        d: 24 * 60 * 60 * 1000,
        w: 7 * 24 * 60 * 60 * 1000,
        mo: 30 * 24 * 60 * 60 * 1000
      };
      return amount > 0 ? amount * multipliers[match[2]] : 24 * 60 * 60 * 1000;
    }
    function rowFingerprint(row) {
      return [
        row.agent,
        row.agent_ip || '',
        row.target_name,
        row.address,
        row.port,
        JSON.stringify(row.labels || []),
        row.checked_at,
        row.success_count,
        row.failure_count,
        row.average_latency_ms,
        row.success_rate,
        row.error || ''
      ].join('|');
    }
    function normalizeResultRows(rows) {
      if (!Array.isArray(rows)) return [];
      return rows.map(normalizeResultRow).filter(Boolean);
    }
    function normalizeResultRow(row) {
      if (!Array.isArray(row)) return row && typeof row === 'object' ? row : null;
      return {
        agent: row[0] || '',
        agent_ip: row[1] || '',
        target_name: row[2] || '',
        address: row[3] || '',
        port: row[4] || 0,
        labels: Array.isArray(row[5]) ? row[5] : [],
        checked_at: decodeCompactTime(row[6]),
        success_count: row[7] || 0,
        failure_count: row[8] || 0,
        average_latency_ms: row[9] || 0,
        success_rate: row[10] || 0,
        error: row[11] || ''
      };
    }
    function decodeCompactTime(value) {
      if (typeof value !== 'string' || value.length < 10 || !/^[0-9a-z]+$/i.test(value)) return value || '';
      const ns = parseBase36BigInt(value);
      if (ns === null) return value;
      const billion = 1000000000n;
      const million = 1000000n;
      const seconds = ns / billion;
      const nanos = ns % billion;
      const millis = seconds * 1000n + nanos / million;
      const date = new Date(Number(millis));
      if (Number.isNaN(date.getTime())) return value;
      const whole = date.toISOString().replace(/\.\d{3}Z$/, '');
      if (nanos === 0n) return whole + 'Z';
      const fraction = nanos.toString().padStart(9, '0').replace(/0+$/, '');
      return whole + '.' + fraction + 'Z';
    }
    function parseBase36BigInt(value) {
      let total = 0n;
      for (const char of value.toLowerCase()) {
        const code = char.charCodeAt(0);
        let digit;
        if (code >= 48 && code <= 57) digit = code - 48;
        else if (code >= 97 && code <= 122) digit = code - 87;
        else return null;
        total = total * 36n + BigInt(digit);
      }
      return total;
    }
    function rowsForCurrentView(rows) {
      const cutoff = Date.now() - parseRangeMillis(selectedRange);
      return rows.filter(row => {
        const ts = timeValue(row);
        return ts !== null && ts >= cutoff && (!selectedAgent || row.agent === selectedAgent);
      });
    }
    function sortRowsByTime(rows) {
      return rows.slice().sort((a, b) => (timeValue(a) || 0) - (timeValue(b) || 0));
    }
    function mergeRows(existing, incoming) {
      const seen = new Set(existing.map(rowFingerprint));
      const merged = existing.slice();
      for (const row of rowsForCurrentView(incoming)) {
        const fingerprint = rowFingerprint(row);
        if (seen.has(fingerprint)) continue;
        seen.add(fingerprint);
        merged.push(row);
      }
      return sortRowsByTime(rowsForCurrentView(merged));
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
    function splitLongGaps(points, typicalGap) {
      if (points.length < 2) return points;
      const threshold = Math.max(minChartGapMs, (typicalGap || medianInterval(points)) * 3);
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
      if (range.endsWith('m')) return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
      if (range.endsWith('h')) return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
      if (range === '24h') return date.toLocaleString([], {month: '2-digit', day: '2-digit', hour: '2-digit'});
      return date.toLocaleDateString([], {month: '2-digit', day: '2-digit'});
    }
    class SvgLineChart {
      constructor(container, data, options) {
        this.container = container;
        this.data = data || {datasets: []};
        this.options = options || {};
        this.visibility = new Map();
        this.raf = 0;
        this.svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        this.svg.setAttribute('role', 'img');
        this.svg.setAttribute('aria-label', '延迟图表');
        this.container.replaceChildren(this.svg);
        this.canvas = this.container;
        this.hoverX = null;
        this.hoverLine = null;
        if (!this.options.mini) {
          this.hoverLine = document.createElement('div');
          this.hoverLine.className = 'chart-hover-line';
          this.container.appendChild(this.hoverLine);
        }
        this.tooltipCache = new Map();
        this.lastTooltipX = null;
        this.lastTooltipPixel = null;
        this.lastPointerClientY = null;
        this.lastArea = null;
        this.lastXRange = null;
        this.tooltipActive = false;
        this.scales = {x: {getValueForPixel: pixel => this.valueForPixel(pixel)}};
        this.resizeObserver = new ResizeObserver(() => this.update());
        this.resizeObserver.observe(this.container);
        this.container.addEventListener('pointermove', event => this.scheduleTooltip(event));
        this.container.addEventListener('pointerleave', () => hideChartTooltip(this));
        if (!this.options.deferUpdate) this.update();
      }
      destroy() {
        cancelAnimationFrame(this.raf);
        cancelAnimationFrame(this.tooltipRaf);
        if (this.resizeObserver) this.resizeObserver.disconnect();
        this.container.replaceChildren();
      }
      setData(data) {
        this.data = data || {datasets: []};
        this.invalidateTooltipCache();
        this.update();
      }
      setDatasetVisibility(index, visible) {
        this.visibility.set(index, visible);
        this.invalidateTooltipCache();
      }
      isDatasetVisible(index) {
        return this.visibility.has(index) ? this.visibility.get(index) : true;
      }
      chartArea() {
        const mini = this.options.mini;
        const rect = this.container.getBoundingClientRect();
        const width = Math.max(1, rect.width || this.container.clientWidth || 1);
        const height = Math.max(1, rect.height || this.container.clientHeight || (mini ? 86 : 390));
        const margin = mini ? {top: 6, right: 4, bottom: 6, left: 4} : {top: 14, right: 18, bottom: 30, left: 42};
        return {width, height, margin, left: margin.left, right: width - margin.right, top: margin.top, bottom: height - margin.bottom};
      }
      visibleDatasets() {
        return this.data.datasets.filter((_, index) => this.isDatasetVisible(index));
      }
      invalidateTooltipCache() {
        this.tooltipCache.clear();
        this.lastTooltipX = null;
        this.lastTooltipPixel = null;
      }
      xRange() {
        const xOptions = this.options.scales?.x || {};
        if (Number.isFinite(xOptions.min) && Number.isFinite(xOptions.max) && xOptions.min < xOptions.max) {
          return {min: xOptions.min, max: xOptions.max};
        }
        return dataRange(this.data) || {min: Date.now() - 1, max: Date.now()};
      }
      yRange(xRange) {
        let max = 0;
        this.visibleDatasets().forEach(dataset => {
          this.pointWindow(dataset.data, xRange).forEach(point => {
            if (point.x < xRange.min || point.x > xRange.max) return;
            if (point.y !== null && Number.isFinite(point.y)) max = Math.max(max, point.y);
          });
        });
        const padded = max > 0 ? max * 1.08 : 1;
        const step = this.niceStep(padded / 5);
        return {min: 0, max: Math.ceil(padded / step) * step};
      }
      valueForPixel(pixel, area, range) {
        area = area || this.lastArea || this.chartArea();
        range = range || this.lastXRange || this.xRange();
        const usable = Math.max(1, area.right - area.left);
        const ratio = Math.min(1, Math.max(0, (pixel - area.left) / usable));
        return range.min + (range.max - range.min) * ratio;
      }
      pointToPixel(point, xRange, yRange, area) {
        const x = area.left + (point.x - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
        const y = area.bottom - (point.y - yRange.min) / Math.max(1, yRange.max - yRange.min) * (area.bottom - area.top);
        return {x, y};
      }
      segmentPath(segment) {
        if (!segment.length) return '';
        if (segment.length === 1) return 'M' + segment[0].x.toFixed(1) + ' ' + segment[0].y.toFixed(1);
        if (segment.length === 2 || this.options.smooth === false) {
          return segment.map((point, index) => (index ? 'L' : 'M') + point.x.toFixed(1) + ' ' + point.y.toFixed(1)).join('');
        }
        const slopes = [];
        const tangents = new Array(segment.length).fill(0);
        for (let i = 0; i < segment.length - 1; i++) {
          const dx = segment[i + 1].x - segment[i].x;
          slopes[i] = dx === 0 ? 0 : (segment[i + 1].y - segment[i].y) / dx;
        }
        tangents[0] = slopes[0];
        tangents[segment.length - 1] = slopes[slopes.length - 1];
        for (let i = 1; i < segment.length - 1; i++) {
          tangents[i] = slopes[i - 1] * slopes[i] <= 0 ? 0 : (slopes[i - 1] + slopes[i]) / 2;
        }
        for (let i = 0; i < slopes.length; i++) {
          if (slopes[i] === 0) {
            tangents[i] = 0;
            tangents[i + 1] = 0;
            continue;
          }
          const a = tangents[i] / slopes[i];
          const b = tangents[i + 1] / slopes[i];
          const h = Math.hypot(a, b);
          if (h > 3) {
            const scale = 3 / h;
            tangents[i] = scale * a * slopes[i];
            tangents[i + 1] = scale * b * slopes[i];
          }
        }
        let d = 'M' + segment[0].x.toFixed(1) + ' ' + segment[0].y.toFixed(1);
        for (let i = 0; i < segment.length - 1; i++) {
          const current = segment[i];
          const next = segment[i + 1];
          const dx = next.x - current.x;
          const c1x = current.x + dx / 3;
          const c1y = current.y + tangents[i] * dx / 3;
          const c2x = next.x - dx / 3;
          const c2y = next.y - tangents[i + 1] * dx / 3;
          d += 'C' + c1x.toFixed(1) + ' ' + c1y.toFixed(1) + ' ' + c2x.toFixed(1) + ' ' + c2y.toFixed(1) + ' ' + next.x.toFixed(1) + ' ' + next.y.toFixed(1);
        }
        return d;
      }
      pointWindow(points, xRange) {
        let start = 0;
        let end = points.length;
        let lo = 0;
        let hi = points.length;
        while (lo < hi) {
          const mid = Math.floor((lo + hi) / 2);
          if (points[mid].x < xRange.min) lo = mid + 1; else hi = mid;
        }
        start = Math.max(0, lo - 1);
        lo = 0;
        hi = points.length;
        while (lo < hi) {
          const mid = Math.floor((lo + hi) / 2);
          if (points[mid].x <= xRange.max) lo = mid + 1; else hi = mid;
        }
        end = Math.min(points.length, lo + 1);
        return points.slice(start, end);
      }
      pathFor(dataset, xRange, yRange, area) {
        let d = '';
        let segment = [];
        const flush = () => {
          if (segment.length) d += this.segmentPath(segment);
          segment = [];
        };
        for (const point of this.pointWindow(dataset.data, xRange)) {
          if (point.y === null || !Number.isFinite(point.y) || point.x < xRange.min || point.x > xRange.max) {
            flush();
            continue;
          }
          segment.push(this.pointToPixel(point, xRange, yRange, area));
        }
        flush();
        return d;
      }
      appendEl(parent, name, attrs) {
        const el = document.createElementNS('http://www.w3.org/2000/svg', name);
        Object.entries(attrs || {}).forEach(([key, value]) => el.setAttribute(key, String(value)));
        parent.appendChild(el);
        return el;
      }
      niceStep(rawStep) {
        const exponent = Math.floor(Math.log10(Math.max(1, rawStep)));
        const base = Math.pow(10, exponent);
        const fraction = rawStep / base;
        const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
        return niceFraction * base;
      }
      yTicks(yRange) {
        const targetCount = 5;
        const max = Math.max(1, yRange.max);
        const step = this.niceStep(max / targetCount);
        const top = Math.ceil(max / step) * step;
        const ticks = [];
        for (let value = 0; value <= top + step / 2; value += step) ticks.push(value);
        return ticks.length >= 2 ? ticks : [0, top || step];
      }
      xStepMs(span) {
        const steps = [
          60 * 1000,
          5 * 60 * 1000,
          15 * 60 * 1000,
          30 * 60 * 1000,
          60 * 60 * 1000,
          2 * 60 * 60 * 1000,
          3 * 60 * 60 * 1000,
          6 * 60 * 60 * 1000,
          12 * 60 * 60 * 1000,
          24 * 60 * 60 * 1000,
          2 * 24 * 60 * 60 * 1000,
          7 * 24 * 60 * 60 * 1000,
          14 * 24 * 60 * 60 * 1000,
          30 * 24 * 60 * 60 * 1000
        ];
        const target = window.innerWidth <= 760 ? 4 : 7;
        const dataStep = this.dataStepMs();
        return steps.find(step => step >= dataStep && span / step <= target) || steps[steps.length - 1];
      }
      dataStepMs() {
        const gaps = this.visibleDatasets()
          .map(dataset => dataset.typicalGap)
          .filter(gap => Number.isFinite(gap) && gap > 0)
          .sort((a, b) => a - b);
        return gaps.length ? gaps[Math.floor(gaps.length / 2)] : minChartGapMs;
      }
      xTicks(xRange) {
        const span = Math.max(1, xRange.max - xRange.min);
        const step = this.xStepMs(span);
        const ticks = [xRange.min];
        let value = Math.ceil(xRange.min / step) * step;
        while (value < xRange.max) {
          if (value > xRange.min) ticks.push(value);
          value += step;
        }
        if (ticks[ticks.length - 1] !== xRange.max) ticks.push(xRange.max);
        return ticks;
      }
      visibleXTicks(ticks, xRange, area) {
        const placed = [];
        const measure = value => Math.max(38, formatTimeTick(value).length * 7 + 10);
        const pixelFor = value => area.left + (value - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
        for (const value of ticks) {
          const x = pixelFor(value);
          const width = measure(value);
          const start = x - width / 2;
          const end = x + width / 2;
          const last = placed[placed.length - 1];
          if (!last || start > last.end + 8 || value === xRange.max) {
            if (value === xRange.max && last && start <= last.end + 8 && last.value !== xRange.min) placed.pop();
            placed.push({value, x, start, end});
          }
        }
        return placed.map(item => item.value);
      }
      renderAxes(area, xRange, yRange) {
        const grid = this.appendEl(this.svg, 'g', {stroke: '#e7ebf1', 'stroke-width': 1});
        const labels = this.appendEl(this.svg, 'g', {fill: '#64748b', 'font-size': 11});
        this.yTicks(yRange).forEach(value => {
          const y = area.bottom - (value - yRange.min) / Math.max(1, yRange.max - yRange.min) * (area.bottom - area.top);
          if (y < area.top - 1 || y > area.bottom + 1) return;
          this.appendEl(grid, 'line', {x1: area.left, x2: area.right, y1: y, y2: y});
          const text = this.appendEl(labels, 'text', {x: 4, y, 'dominant-baseline': value === 0 ? 'auto' : 'middle'});
          text.textContent = value === 0 ? '0' : Math.round(value) + 'ms';
        });
        const xTicks = this.visibleXTicks(this.xTicks(xRange), xRange, area);
        xTicks.forEach((value, index) => {
          const x = area.left + (value - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
          const anchor = index === 0 ? 'start' : index === xTicks.length - 1 ? 'end' : 'middle';
          const text = this.appendEl(labels, 'text', {x, y: area.height - 8, 'text-anchor': anchor});
          text.textContent = formatTimeTick(value);
        });
      }
      update() {
        this.invalidateTooltipCache();
        cancelAnimationFrame(this.raf);
        this.raf = requestAnimationFrame(() => {
          const area = this.chartArea();
          const xRange = this.xRange();
          const yRange = this.yRange(xRange);
          this.lastArea = area;
          this.lastXRange = xRange;
          this.svg.setAttribute('viewBox', '0 0 ' + area.width + ' ' + area.height);
          this.svg.replaceChildren();
          if (!this.options.mini) this.renderAxes(area, xRange, yRange);
          const lines = this.appendEl(this.svg, 'g', {fill: 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'});
          this.data.datasets.forEach((dataset, index) => {
            if (!this.isDatasetVisible(index)) return;
            const d = this.pathFor(dataset, xRange, yRange, area);
            if (!d) return;
            this.appendEl(lines, 'path', {d, stroke: dataset.borderColor, 'stroke-width': this.options.mini ? 1.6 : 2});
          });
          this.updateHoverLine();
        });
      }
      nearestAnchorTime(xValue) {
        let best = null;
        this.visibleDatasets().forEach(dataset => {
          const nearest = this.nearestInDataset(dataset, xValue);
          if (!nearest) return;
          if (!best || nearest.distance < best.distance) best = nearest;
        });
        return best ? best.point.x : null;
      }
      updateHoverLine() {
        if (!this.hoverLine) return;
        const area = this.lastArea || this.chartArea();
        const xRange = this.lastXRange || this.xRange();
        if (this.hoverX === null || this.hoverX < xRange.min || this.hoverX > xRange.max) {
          this.hoverLine.style.opacity = '0';
          return;
        }
        const x = area.left + (this.hoverX - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
        this.hoverLine.style.top = area.top.toFixed(1) + 'px';
        this.hoverLine.style.bottom = (area.height - area.bottom).toFixed(1) + 'px';
        this.hoverLine.style.opacity = '1';
        this.hoverLine.style.transform = 'translate3d(' + x.toFixed(1) + 'px, 0, 0)';
      }
      nearestInDataset(dataset, xValue) {
        const points = dataset.data;
        let lo = 0;
        let hi = points.length - 1;
        while (lo < hi) {
          const mid = Math.floor((lo + hi) / 2);
          if (points[mid].x < xValue) lo = mid + 1; else hi = mid;
        }
        let best = null;
        [lo - 2, lo - 1, lo, lo + 1, lo + 2].forEach(index => {
          const point = points[index];
          if (!point || point.y === null) return;
          const distance = Math.abs(point.x - xValue);
          if (!best || distance < best.distance) best = {dataset, point, distance};
        });
        return best;
      }
      tooltipPoints(clientX) {
        const rect = this.container.getBoundingClientRect();
        const pointerX = this.valueForPixel(clientX - rect.left, this.lastArea, this.lastXRange);
        const xValue = this.nearestAnchorTime(pointerX);
        if (xValue === null) return {xValue: pointerX, items: []};
        if (this.tooltipCache.has(xValue)) return this.tooltipCache.get(xValue);
        const items = [];
        this.visibleDatasets().forEach(dataset => {
          const nearest = this.nearestInDataset(dataset, xValue);
          if (!nearest) return;
          const typicalGap = dataset.typicalGap || minChartGapMs;
          const maxDistance = Math.max(minChartGapMs, typicalGap * 1.5);
          if (nearest.distance <= maxDistance) items.push(nearest);
        });
        items.sort((a, b) => b.point.y - a.point.y || a.dataset.label.localeCompare(b.dataset.label));
        const result = {xValue, items};
        this.tooltipCache.set(xValue, result);
        return result;
      }
      scheduleTooltip(event) {
        const clientX = event.clientX;
        const clientY = event.clientY;
        this.tooltipActive = true;
        const pixel = Math.round(clientX);
        if (pixel === this.lastTooltipPixel && Math.abs(clientY - (this.lastPointerClientY || clientY)) < 2) return;
        this.lastTooltipPixel = pixel;
        this.lastPointerClientY = clientY;
        cancelAnimationFrame(this.tooltipRaf);
        this.tooltipRaf = requestAnimationFrame(() => {
          if (this.tooltipActive) showSvgTooltip(this, clientX, clientY);
        });
      }
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
        const typicalGap = medianInterval(points);
        const displayPoints = splitLongGaps(points, typicalGap);
        return {
          label,
          data: displayPoints,
          borderColor: colors[index % colors.length],
          backgroundColor: colors[index % colors.length],
          typicalGap,
          tension: 0.18,
          cubicInterpolationMode: 'monotone',
          pointRadius: 0,
          pointHoverRadius: 4,
          pointHitRadius: 10,
          borderWidth: 2,
          spanGaps: false,
          parsing: false,
          normalized: true
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
      const area = chart.chartArea();
      const width = Math.max(1, area.right - area.left);
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
      if (chart) {
        chart.tooltipActive = false;
        cancelAnimationFrame(chart.tooltipRaf);
        chart.hoverX = null;
        chart.lastTooltipPixel = null;
        chart.lastPointerClientY = null;
        chart.updateHoverLine();
      }
    }
    function showSvgTooltip(chart, clientX, clientY) {
      const tooltipData = chart.tooltipPoints(clientX);
      const tooltip = chartTooltip();
      if (!tooltipData.items.length) {
        tooltip.style.opacity = '0';
        chart.hoverX = null;
        chart.updateHoverLine();
        chart.lastTooltipX = null;
        return;
      }
      chart.hoverX = tooltipData.xValue;
      chart.updateHoverLine();
      const positionTooltip = () => {
        tooltip.style.opacity = '1';
        tooltip.style.left = '0px';
        tooltip.style.top = '0px';
        const tooltipRect = tooltip.getBoundingClientRect();
        let left = clientX + 14;
        let top = clientY + 14;
        if (left + tooltipRect.width > window.innerWidth - 12) {
          left = clientX - tooltipRect.width - 14;
        }
        if (top + tooltipRect.height > window.innerHeight - 12) {
          top = window.innerHeight - tooltipRect.height - 12;
        }
        tooltip.style.left = Math.max(12, left) + 'px';
        tooltip.style.top = Math.max(12, top) + 'px';
      };
      if (chart.lastTooltipX === tooltipData.xValue) {
        positionTooltip();
        return;
      }
      chart.lastTooltipX = tooltipData.xValue;
      tooltip.innerHTML = '';
      const title = document.createElement('div');
      title.className = 'chart-tooltip-title';
      title.textContent = new Date(tooltipData.xValue).toLocaleString();
      tooltip.appendChild(title);
      const compactTooltip = window.matchMedia('(max-width: 760px), (pointer: coarse)').matches;
      const maxItems = compactTooltip ? 8 : 18;
      tooltipData.items.slice(0, maxItems).forEach(item => {
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
        value.textContent = item.point.y.toFixed(2) + ' ms';
        row.append(swatch, name, value);
        tooltip.appendChild(row);
      });
      const hiddenCount = tooltipData.items.length - maxItems;
      if (hiddenCount > 0) {
        const more = document.createElement('div');
        more.className = 'chart-tooltip-more';
        more.textContent = '还有 ' + hiddenCount + ' 项';
          tooltip.appendChild(more);
        }
      positionTooltip();
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
    function findAgentStatus(agent) {
      return currentAgents.find(item => item.agent === agent) || null;
    }
    function agentStatusLabel(status, summary) {
      const lastSeen = agentLastSeenTime(status, summary);
      const offlineAfter = Number((status && status.offline_after_seconds) || defaultOfflineAfterSeconds || 90) * 1000;
      if (lastSeen !== null && Date.now() - lastSeen > offlineAfter) return {text: '离线', className: 'offline'};
      if (!summary) return {text: '暂无数据', className: 'idle'};
      return summary.successRate >= 0.99 ? {text: '正常', className: 'ok'} : {text: '异常', className: 'bad'};
    }
    function agentStateBadgeHTML(state) {
      return '<span class="status ' + state.className + '">' + state.text + '</span>';
    }
    function agentLastSeenTime(status, summary) {
      const raw = status && status.last_seen_at ? status.last_seen_at : summary && summary.latest && summary.latest.checked_at;
      if (!raw) return null;
      const ts = new Date(raw).getTime();
      return Number.isNaN(ts) ? null : ts;
    }
    function lastSeenText(status, summary) {
      const ts = agentLastSeenTime(status, summary);
      return ts === null ? '未知' : new Date(ts).toLocaleString();
    }
    function agentIPText(status, summary) {
      return (status && status.agent_ip) || (summary && summary.latest && summary.latest.agent_ip) || '未知';
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
    function scheduleLowPriority(fn) {
      if ('requestIdleCallback' in window) {
        window.requestIdleCallback(fn, {timeout: 800});
        return;
      }
      requestAnimationFrame(() => setTimeout(fn, 0));
    }
    function destroyMiniCharts() {
      if (miniChartObserver) {
        miniChartObserver.disconnect();
        miniChartObserver = null;
      }
      miniCharts.forEach(chart => chart.destroy());
      miniCharts.clear();
    }
    function destroyMiniChartSurface(surface) {
      if (!surface) return;
      if (miniChartObserver) miniChartObserver.unobserve(surface);
      if (surface.__miniChart) {
        miniCharts.delete(surface.__miniChart);
        surface.__miniChart.destroy();
        delete surface.__miniChart;
      }
      delete surface.__chartRows;
    }
    function renderMiniChart(surface, rows) {
      if (!surface.isConnected) return;
      const chartData = buildDatasets(rows);
      if (surface.__miniChart) {
        surface.__miniChart.setData(chartData);
        return;
      }
      const miniChart = new SvgLineChart(surface, chartData, {mini: true, smooth: true, scales: {x: {}}});
      surface.__miniChart = miniChart;
      miniCharts.add(miniChart);
    }
    function queueMiniChart(surface, rows) {
      if (!surface) return;
      if (surface.__miniChart) {
        surface.__miniChart.setData(buildDatasets(rows));
        return;
      }
      if (!('IntersectionObserver' in window)) {
        scheduleLowPriority(() => renderMiniChart(surface, rows));
        return;
      }
      if (!miniChartObserver) {
        miniChartObserver = new IntersectionObserver(entries => {
          entries.forEach(entry => {
            if (!entry.isIntersecting) return;
            if (miniChartObserver) miniChartObserver.unobserve(entry.target);
            const rows = entry.target.__chartRows || [];
            delete entry.target.__chartRows;
            scheduleLowPriority(() => renderMiniChart(entry.target, rows));
          });
        }, {rootMargin: '320px 0px'});
      }
      surface.__chartRows = rows;
      miniChartObserver.observe(surface);
    }
    function renderLanding(rows) {
      const wrap = document.getElementById('agentCards');
      const existingCards = new Map(Array.from(wrap.querySelectorAll('.agent-card')).map(card => [card.dataset.agent, card]));
      const groups = new Map();
      for (const row of rows) {
        if (!groups.has(row.agent)) groups.set(row.agent, []);
        groups.get(row.agent).push(row);
      }
      const agentNames = new Set(currentAgents.map(agent => agent.agent).filter(Boolean));
      groups.forEach((_, agent) => agentNames.add(agent));
      if (!agentNames.size) {
        destroyMiniCharts();
        wrap.innerHTML = '<div class="panel">暂无节点在线数据</div>';
        return;
      }
      wrap.querySelectorAll('.panel').forEach(panel => panel.remove());
      const activeAgents = new Set();
      Array.from(agentNames).sort((a, b) => a.localeCompare(b)).forEach(agent => {
        const agentRows = groups.get(agent) || [];
        const summary = agentRows.length ? summarizeAgent(agentRows) : null;
        const statusInfo = findAgentStatus(agent);
        const state = agentStatusLabel(statusInfo, summary);
        const detailURL = '/dashboard?agent=' + encodeURIComponent(agent) + '&range=' + encodeURIComponent(selectedRange);
        activeAgents.add(agent);
        let card = existingCards.get(agent);
        if (!card) {
          card = document.createElement('div');
          card.className = 'agent-card';
          card.tabIndex = 0;
          card.setAttribute('role', 'link');
          card.innerHTML =
            '<div class="card-head"><div><div class="agent-name"></div><div class="subtle"></div></div><span class="status"></span></div>' +
            '<div class="metrics"></div><div class="mini-chart chart-surface"></div>';
          card.addEventListener('click', event => {
            location.href = card.dataset.href;
          });
          card.addEventListener('keydown', event => {
            if (event.key !== 'Enter' && event.key !== ' ') return;
            event.preventDefault();
            location.href = card.dataset.href;
          });
        }
        card.dataset.agent = agent;
        card.dataset.href = detailURL;
        card.querySelector('.agent-name').textContent = agent;
        card.querySelector('.subtle').textContent = '节点 IP：' + agentIPText(statusInfo, summary) + ' · 最后在线：' + lastSeenText(statusInfo, summary);
        const status = card.querySelector('.status');
        status.textContent = state.text;
        status.className = 'status ' + state.className;
        const metrics = card.querySelector('.metrics');
        metrics.replaceChildren();
        metrics.append(metric('目标数', summary ? String(summary.targetCount) : '0'));
        metrics.append(metric('成功率', summary ? (summary.successRate * 100).toFixed(1) + '%' : '--'));
        metrics.append(metric('平均延迟', summary ? summary.averageLatency.toFixed(1) + ' ms' : '--'));
        wrap.appendChild(card);
        const chartSurface = card.querySelector('.mini-chart');
        if (agentRows.length) {
          queueMiniChart(chartSurface, agentRows);
        } else {
          destroyMiniChartSurface(chartSurface);
          chartSurface.innerHTML = '';
        }
      });
      existingCards.forEach((card, agent) => {
        if (activeAgents.has(agent)) return;
        destroyMiniChartSurface(card.querySelector('.mini-chart'));
        card.remove();
      });
    }
    function renderAgentInfo(rows) {
      const wrap = document.getElementById('agentInfo');
      wrap.innerHTML = '';
      const statusInfo = findAgentStatus(selectedAgent);
      const summary = rows.length ? summarizeAgent(rows) : null;
      const state = agentStatusLabel(statusInfo, summary);
      const deleteButton = document.getElementById('deleteAgentButton');
      if (deleteButton) deleteButton.hidden = state.className !== 'offline';
      const subtitle = document.getElementById('pageSubtitle');
      subtitle.innerHTML = '最后在线：' + lastSeenText(statusInfo, summary) + '<span class="live-badge" id="liveState">实时</span>' + agentStateBadgeHTML(state);
      wrap.append(metric('节点名称', (summary && summary.latest.agent) || (statusInfo && statusInfo.agent) || selectedAgent));
      wrap.append(metric('节点 IP', agentIPText(statusInfo, summary)));
      wrap.append(metric('监测目标', summary ? String(summary.targetCount) : '0'));
      wrap.append(metric('最后在线', lastSeenText(statusInfo, summary)));
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
      const visibleLabels = new Set(chart.data.datasets.map(dataset => dataset.label));
      targetVisibility = new Map(Array.from(targetVisibility.entries()).filter(([label]) => visibleLabels.has(label)));
      chart.data.datasets.forEach((dataset, index) => {
        const label = document.createElement('label');
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.checked = targetVisibility.has(dataset.label) ? targetVisibility.get(dataset.label) : true;
        chart.setDatasetVisibility(index, input.checked);
        input.addEventListener('change', () => {
          targetVisibility.set(dataset.label, input.checked);
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
        renderToggles(detailChart);
        applyDetailChartRange('none');
        return;
      }
      detailChart = new SvgLineChart(document.getElementById('latency'), chartData, {mini: false, smooth: true, deferUpdate: true, scales: {x: {}}});
      attachChartZoomHandlers(detailChart);
      renderToggles(detailChart);
      applyDetailChartRange('none');
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
    function renderDashboardRows(rows) {
      currentRows = sortRowsByTime(rowsForCurrentView(rows));
      if (!selectedAgent) {
        renderLanding(currentRows);
        return;
      }
      if (!currentRows.length) {
        currentAgentRows = [];
        renderAgentInfo(currentRows);
        renderProblemLog(currentRows);
        renderLabelFilters(currentRows);
        updateDetailChart(currentRows);
        return;
      }
      currentAgentRows = currentRows;
      renderAgentInfo(currentRows);
      renderProblemLog(currentRows);
      renderLabelFilters(currentRows);
      updateDetailChart(currentRows);
    }
    function scheduleDashboardRender(rows) {
      pendingDashboardRows = rows;
      if (renderDashboardTimer !== null) return;
      renderDashboardTimer = window.setTimeout(() => {
        renderDashboardTimer = null;
        const rows = pendingDashboardRows;
        pendingDashboardRows = null;
        const scrollX = window.scrollX;
        const scrollY = window.scrollY;
        renderDashboardRows(rows);
        requestAnimationFrame(() => window.scrollTo(scrollX, scrollY));
      }, 120);
    }
    async function refreshDashboard() {
      const scrollX = window.scrollX;
      const scrollY = window.scrollY;
      const [rows, agents] = await Promise.all([loadResults(), loadAgents()]);
      currentAgents = agents;
      renderDashboardRows(rows);
      requestAnimationFrame(() => window.scrollTo(scrollX, scrollY));
    }
    async function applyLiveResults(rows) {
      rows = normalizeResultRows(rows);
      if (!rows.length) return;
      currentRows = mergeRows(currentRows, rows);
      try {
        currentAgents = await loadAgents();
      } catch (err) {
        console.warn(err);
      }
      scheduleDashboardRender(currentRows);
    }
    function handleRefreshError(err) {
      const targetToggles = document.getElementById('targetToggles');
      const agentCards = document.getElementById('agentCards');
      if (targetToggles) targetToggles.textContent = '加载失败：' + err.message;
      if (agentCards) agentCards.innerHTML = '<div class="panel">加载失败：' + err.message + '</div>';
    }
    function updateLiveState(text, reconnecting) {
      const liveState = document.getElementById('liveState');
      if (!liveState) return;
      liveState.textContent = text;
      liveState.classList.toggle('reconnecting', reconnecting);
    }
    document.getElementById('refreshButton').addEventListener('click', () => {
      refreshDashboard().catch(handleRefreshError);
    });
    const deleteAgentButton = document.getElementById('deleteAgentButton');
    if (deleteAgentButton) {
      deleteAgentButton.addEventListener('click', () => deleteAgent(selectedAgent).catch(handleRefreshError));
    }
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
    const rangeCustomForm = document.getElementById('rangeCustomForm');
    const rangeCustomInput = document.getElementById('rangeCustomInput');
    const rangeCustomApply = document.getElementById('rangeCustomApply');
    const backButton = document.getElementById('backButton');
    const rangePresets = new Set(Array.from(document.querySelectorAll('.range-option')).map(option => option.dataset.range));
    const rangeCookieName = 'pingmon_range';
    const customRangeCookieName = 'pingmon_custom_range';
    rangeButton.addEventListener('click', () => rangeMenu.classList.toggle('open'));
    function setCookie(name, value, maxAgeSeconds) {
      document.cookie = name + '=' + encodeURIComponent(value) + '; Max-Age=' + maxAgeSeconds + '; Path=/; SameSite=Lax';
    }
    function getCookie(name) {
      const prefix = name + '=';
      return document.cookie.split(';').map(part => part.trim()).find(part => part.startsWith(prefix))?.slice(prefix.length) || '';
    }
    function normalizeRange(raw) {
      const value = String(raw || '').trim().toLowerCase();
      return /^\d+(m|h|d|w|mo)$/.test(value) && parseRangeMillis(value) > 0 ? value : '';
    }
    function applyRange(nextRange) {
      selectedRange = nextRange;
      rangeButton.textContent = selectedRange;
      setCookie(rangeCookieName, selectedRange, 365 * 24 * 60 * 60);
      if (rangeCustomInput) rangeCustomInput.value = rangePresets.has(selectedRange) ? '' : selectedRange;
      if (rangePresets.has(selectedRange)) {
        setCookie(customRangeCookieName, '', 0);
      } else {
        setCookie(customRangeCookieName, selectedRange, 365 * 24 * 60 * 60);
      }
      document.querySelectorAll('.range-option').forEach(item => item.classList.toggle('active', item.dataset.range === selectedRange));
      rangeMenu.classList.remove('open');
      rangeMenu.classList.remove('invalid');
      hideChartTooltip(detailChart);
      chartViewRange = null;
      const url = new URL(location.href);
      url.searchParams.set('range', selectedRange);
      history.replaceState(null, '', url);
      if (backButton) backButton.href = '/dashboard?range=' + encodeURIComponent(selectedRange);
      refreshDashboard().catch(handleRefreshError);
    }
    document.querySelectorAll('.range-option').forEach(option => {
      option.addEventListener('click', () => {
        applyRange(option.dataset.range);
      });
    });
    if (rangeCustomForm && rangeCustomInput && rangeCustomApply) {
      function submitCustomRange() {
        const nextRange = normalizeRange(rangeCustomInput.value);
        if (!nextRange) {
          rangeMenu.classList.add('invalid');
          rangeCustomInput.focus();
          return;
        }
        applyRange(nextRange);
      }
      rangeCustomApply.addEventListener('click', submitCustomRange);
      rangeCustomInput.addEventListener('keydown', event => {
        if (event.key !== 'Enter') return;
        event.preventDefault();
        submitCustomRange();
      });
      rangeCustomInput.addEventListener('input', () => rangeMenu.classList.remove('invalid'));
      const savedCustomRange = normalizeRange(decodeURIComponent(getCookie(customRangeCookieName)));
      if (savedCustomRange && rangePresets.has(selectedRange)) {
        rangeCustomInput.value = savedCustomRange;
      }
    }
    document.addEventListener('click', event => {
      if (!rangeMenu.contains(event.target)) rangeMenu.classList.remove('open');
      const chartTooltip = document.getElementById('chartTooltip');
      if (chartTooltip && !event.target.closest('.chart-surface')) hideChartTooltip(detailChart);
    });
    document.addEventListener('touchstart', event => {
      if (!rangeMenu.contains(event.target)) rangeMenu.classList.remove('open');
      if (!event.target.closest('.chart-surface')) hideChartTooltip(detailChart);
    }, {passive: true});
    window.addEventListener('resize', () => {
      rangeMenu.classList.remove('open');
      hideChartTooltip(detailChart);
    });
    refreshDashboard().catch(handleRefreshError);
    window.setInterval(async () => {
      try {
        currentAgents = await loadAgents();
        renderDashboardRows(currentRows);
      } catch (err) {
        console.warn(err);
      }
    }, 15000);
    function connectLiveRefresh() {
      const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
      const ws = new WebSocket(proto + location.host + '/ws');
      ws.onopen = () => {
        updateLiveState('实时', false);
      };
      ws.onmessage = event => {
        if (event.data === 'connected') return;
        if (event.data === 'refresh') {
          refreshDashboard().catch(handleRefreshError);
          return;
        }
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'results') applyLiveResults(message.results);
        } catch (err) {
          console.warn('未知实时消息', err);
        }
      };
      ws.onclose = () => {
        updateLiveState('重连中', true);
        setTimeout(connectLiveRefresh, 3000);
      };
      ws.onerror = () => ws.close();
    }
    connectLiveRefresh();
  </script>
</body>
</html>`
