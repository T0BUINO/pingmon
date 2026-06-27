package main

import (
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"
	"pingmon/internal/storage"

	"github.com/gorilla/websocket"
)

type server struct {
	cfg          config.Config
	store        storage.Store
	tpl          *template.Template
	hub          *websocketHub
	resultsCache *resultsCache
	authKey      []byte
}

type dashboardData struct {
	Agent               string
	Ranges              []string
	SelectedRange       string
	CustomRange         bool
	OfflineAfterSeconds int
	RollupIntervalMins  int
}

type websocketHub struct {
	mu      sync.Mutex
	clients map[*wsClient]struct{}
}

type wsClient struct {
	conn *websocket.Conn
	send chan []byte
	done chan struct{}
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

const resultsCacheTTL = 2 * time.Second
const resultsCacheMaxEntries = 256
const dashboardSessionLifetime = 12 * time.Hour

var websocketUpgrader = websocket.Upgrader{
	HandshakeTimeout: 5 * time.Second,
	ReadBufferSize:   1024,
	WriteBufferSize:  4096,
}

//go:embed dashboard.html
var dashboardHTML string

//go:embed dashboard.css
var dashboardCSS string

//go:embed dashboard.js
var dashboardJS string

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
	tpl := template.Must(template.New("dashboard").Parse(dashboardHTML))
	s := &server{
		cfg:          cfg,
		store:        store,
		tpl:          tpl,
		hub:          newWebsocketHub(),
		resultsCache: newResultsCache(resultsCacheTTL, resultsCacheMaxEntries),
		authKey:      dashboardAuthKey(cfg),
	}
	go s.startRetentionCleaner()

	mux := newSupervisorMux(s)

	log.Printf("supervisor listening on %s", cfg.Listen)
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           gzipHandler(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	log.Fatal(httpServer.ListenAndServe())
}

func newSupervisorMux(s *server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	})
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/report", s.handleReport)
	mux.HandleFunc("/api/agents", s.requireDashboardAuth(s.handleAgents))
	mux.HandleFunc("/api/results", s.requireDashboardAuth(s.handleResults))
	mux.HandleFunc("/ws", s.requireDashboardAuth(s.handleWebSocket))
	mux.HandleFunc("/assets/dashboard.css", s.requireDashboardAuth(staticAsset("text/css; charset=utf-8", dashboardCSS)))
	mux.HandleFunc("/assets/dashboard.js", s.requireDashboardAuth(staticAsset("text/javascript; charset=utf-8", dashboardJS)))
	mux.HandleFunc("/login", s.handleLogin)
	mux.HandleFunc("/logout", s.handleLogout)
	mux.HandleFunc("/dashboard", s.handleDashboard)
	return mux
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
	saved, err := s.store.SaveResults(validatedResults, seenAt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.resultsCache.clear()
	s.hub.broadcastJSON(websocketEvent{Type: "results", Results: saved})
	limit := s.cfg.FailureThreshold + 1
	seenTargets := make(map[string]bool, len(saved))
	for _, result := range saved {
		key := result.TargetName + "\x00" + result.Address + "\x00" + strconv.Itoa(result.Port)
		if seenTargets[key] {
			continue
		}
		seenTargets[key] = true
		failures, err := s.store.ConsecutiveFailures(result.TargetName, result.Address, result.Port, limit)
		if err == nil && failures > s.cfg.FailureThreshold {
			log.Printf("[ALERT] target=%s address=%s:%d consecutive_failures=%d threshold=%d",
				result.TargetName, result.Address, result.Port, failures, s.cfg.FailureThreshold)
		}
	}
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
	if !s.dashboardAuthenticated(r) {
		http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
		return
	}
	selectedRange := s.selectedRange(r)
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, max-age=10")
	if err := s.tpl.Execute(w, dashboardData{
		Agent:               agent,
		Ranges:              s.cfg.DashboardRanges,
		SelectedRange:       selectedRange,
		CustomRange:         !rangeInList(selectedRange, s.cfg.DashboardRanges),
		OfflineAfterSeconds: s.agentOfflineAfterSeconds(),
		RollupIntervalMins:  s.cfg.RollupIntervalMins,
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
	if agent == "" {
		if d := rangeDuration(selectedRange); d > 24*time.Hour {
			selectedRange = "24h"
		}
	}
	since := time.Now().Add(-rangeDuration(selectedRange))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, max-age=5")
	cacheKey := resultsCacheKey{selectedRange: selectedRange, agent: agent, dashboard: true}
	if data, ok := s.resultsCache.get(cacheKey); ok {
		writeJSONBytes(w, data)
		return
	}
	data, err := s.dashboardResultsData(since, agent)
	if err != nil {
		log.Printf("stream dashboard results: %v", err)
		http.Error(w, "stream dashboard results", http.StatusInternalServerError)
		return
	}
	s.resultsCache.set(cacheKey, data)
	writeJSONBytes(w, data)
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

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func (s *server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	c := s.hub.add(conn)
	defer s.hub.remove(c)
	conn.SetReadLimit(1024)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func newWebsocketHub() *websocketHub {
	return &websocketHub{clients: make(map[*wsClient]struct{})}
}

func (h *websocketHub) add(conn *websocket.Conn) *wsClient {
	c := &wsClient{conn: conn, send: make(chan []byte, 16), done: make(chan struct{})}
	c.send <- []byte("connected")
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	go h.runWriter(c)
	return c
}

func (h *websocketHub) remove(c *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[c]; !ok {
		h.mu.Unlock()
		return
	}
	delete(h.clients, c)
	h.mu.Unlock()
	close(c.done)
	_ = c.conn.Close()
}

func (h *websocketHub) runWriter(c *wsClient) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case message, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				h.remove(c)
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				h.remove(c)
				return
			}
		}
	}
}

func (h *websocketHub) broadcast(message string) {
	h.broadcastBytes([]byte(message))
}

func (h *websocketHub) broadcastBytes(payload []byte) {
	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		select {
		case <-c.done:
		case c.send <- payload:
		default:
			h.remove(c)
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
	h.broadcastBytes(payload)
}

func (s *server) requireDashboardAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.dashboardAuthenticated(r) {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *server) dashboardAuthenticated(r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	if ok &&
		subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.DashboardUser)) == 1 &&
		subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.DashboardPassword)) == 1 {
		return true
	}
	// Browsers may retain same-name cookies with different Path or Domain
	// attributes. Accept any valid session instead of letting the first stale
	// cookie shadow a valid one later in the Cookie header.
	for _, cookie := range r.CookiesNamed("pingmon_auth") {
		if s.validDashboardCookie(cookie.Value) {
			return true
		}
	}
	return false
}

func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	next := safeRedirectPath(r.FormValue("next"))
	if r.Method == http.MethodGet {
		if s.dashboardAuthenticated(r) {
			http.Redirect(w, r, next, http.StatusSeeOther)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		page := strings.ReplaceAll(loginHTML, "{{NEXT}}", template.HTMLEscapeString(next))
		_, _ = io.WriteString(w, strings.ReplaceAll(page, "{{ERROR}}", ""))
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	user := r.Form.Get("username")
	pass := r.Form.Get("password")
	if subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.DashboardUser)) != 1 ||
		subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.DashboardPassword)) != 1 {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, strings.ReplaceAll(strings.ReplaceAll(loginHTML, "{{NEXT}}", template.HTMLEscapeString(next)), "{{ERROR}}", "用户名或密码错误"))
		return
	}
	s.setDashboardSession(w, r, user)
	http.Redirect(w, r, next, http.StatusSeeOther)
}

func (s *server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "pingmon_auth", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *server) setDashboardSession(w http.ResponseWriter, r *http.Request, user string) {
	expires := time.Now().Add(dashboardSessionLifetime)
	http.SetCookie(w, &http.Cookie{
		Name:     "pingmon_auth",
		Value:    s.dashboardSession(user, expires),
		Path:     "/",
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
		MaxAge:   int(dashboardSessionLifetime.Seconds()),
	})
}

func safeRedirectPath(raw string) string {
	if raw == "" || !strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "//") {
		return "/dashboard"
	}
	return raw
}

// dashboardAuthKey must remain stable across supervisor restarts so a valid
// session cookie does not silently become invalid while it is still alive.
// Changing either dashboard credential intentionally invalidates old sessions.
func dashboardAuthKey(cfg config.Config) []byte {
	key := sha256.Sum256([]byte("pingmon dashboard session\x00" + cfg.DashboardUser + "\x00" + cfg.DashboardPassword))
	return key[:]
}

func (s *server) validDashboardCookie(value string) bool {
	encodedPayload, encodedSignature, ok := strings.Cut(value, ".")
	if !ok {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(encodedPayload)
	if err != nil {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(encodedSignature)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, s.authKey)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return false
	}
	user, expiresText, ok := strings.Cut(string(payload), "\n")
	if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.DashboardUser)) != 1 {
		return false
	}
	expiresUnix, err := strconv.ParseInt(expiresText, 10, 64)
	return err == nil && time.Now().Unix() < expiresUnix
}

func (s *server) dashboardSession(user string, expires time.Time) string {
	payload := []byte(user + "\n" + strconv.FormatInt(expires.Unix(), 10))
	mac := hmac.New(sha256.New, s.authKey)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
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
		log.Printf("retention cleanup rolled=%d deleted_raw=%d deleted_rollups=%d", rolled, deletedRaw, deletedRollups)
	}
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w *gzipResponseWriter) Write(data []byte) (int, error) {
	return w.Writer.Write(data)
}

func gzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) || !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		gz := gzipWriterPool.Get().(*gzip.Writer)
		gz.Reset(w)
		defer func() {
			_ = gz.Close()
			gzipWriterPool.Put(gz)
		}()
		next.ServeHTTP(&gzipResponseWriter{Writer: gz, ResponseWriter: w}, r)
	})
}

var gzipWriterPool = sync.Pool{New: func() any {
	return gzip.NewWriter(io.Discard)
}}

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

func staticAsset(contentType, content string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "private, max-age=3600")
		_, _ = io.WriteString(w, content)
	}
}

type resultsCacheKey struct {
	selectedRange string
	agent         string
	dashboard     bool
}

type resultsCacheEntry struct {
	data      []byte
	expiresAt time.Time
}

type resultsCache struct {
	mu         sync.RWMutex
	ttl        time.Duration
	maxEntries int
	entries    map[resultsCacheKey]resultsCacheEntry
}

func newResultsCache(ttl time.Duration, maxEntries ...int) *resultsCache {
	max := resultsCacheMaxEntries
	if len(maxEntries) > 0 {
		max = maxEntries[0]
	}
	return &resultsCache{
		ttl:        ttl,
		maxEntries: max,
		entries:    make(map[resultsCacheKey]resultsCacheEntry),
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
	now := time.Now()
	for existingKey, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, existingKey)
		}
	}
	if c.maxEntries > 0 && len(c.entries) >= c.maxEntries {
		var oldestKey resultsCacheKey
		var oldestExpiry time.Time
		for existingKey, entry := range c.entries {
			if oldestExpiry.IsZero() || entry.expiresAt.Before(oldestExpiry) {
				oldestKey, oldestExpiry = existingKey, entry.expiresAt
			}
		}
		delete(c.entries, oldestKey)
	}
	c.entries[key] = resultsCacheEntry{
		data:      append([]byte(nil), data...),
		expiresAt: now.Add(c.ttl),
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
