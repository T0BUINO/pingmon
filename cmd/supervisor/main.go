package main

import (
	"compress/gzip"
	"context"
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
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"
	"pingmon/internal/storage"

	"github.com/gorilla/websocket"
)

type server struct {
	cfgMu          sync.RWMutex
	cfg            config.Config
	store          storage.Store
	tpl            *template.Template
	hub            *websocketHub
	resultsCache   *resultsCache
	authKey        []byte
	connectivityMu sync.Mutex
	connectivity   map[string]agentConnectivity
}

type agentConnectivity struct {
	Status              string
	ConsecutiveFailures int
	FirstFailedAt       time.Time
	LastChangedAt       time.Time
}

type dashboardData struct {
	Agent               string
	Ranges              []string
	SelectedRange       string
	CustomRange         bool
	OfflineAfterSeconds int
	RollupIntervalMins  int
	AssetVersion        string
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
	Agent                 string    `json:"agent"`
	AgentIP               string    `json:"agent_ip,omitempty"`
	FirstSeenAt           time.Time `json:"first_seen_at"`
	LastSeenAt            time.Time `json:"last_seen_at"`
	OfflineAfterSeconds   int       `json:"offline_after_seconds"`
	Status                string    `json:"status"`
	Connectivity          string    `json:"connectivity"`
	ConsecutiveFailures   int       `json:"consecutive_failures"`
	ConnectivityChangedAt time.Time `json:"connectivity_changed_at,omitempty"`
}

const resultsCacheTTL = 2 * time.Second
const resultsCacheMaxEntries = 64
const resultsCacheMaxBytes = 16 << 20
const dashboardSessionLifetime = 12 * time.Hour
const maxReportBodyBytes = 1 << 20
const maxReportBatchSize = 1000

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

var embeddedAssetVersion = func() string {
	sum := sha256.Sum256([]byte(dashboardCSS + "\x00" + dashboardJS))
	return fmt.Sprintf("%x", sum[:6])
}()

func main() {
	configPath := flag.String("config", "configs/supervisor.toml", "path to JSON or TOML config")
	format := flag.String("format", "", "config format: json or toml")
	migrateOnly := flag.Bool("migrate-only", false, "migrate SQLite storage and exit")
	flag.Parse()

	cfg, err := config.Load(*configPath, *format)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := validateSupervisorConfig(cfg); err != nil {
		log.Fatalf("validate config: %v", err)
	}
	if cfg.AgentToken == "" {
		log.Printf("WARNING: agent_token is empty; /api/tasks and /api/report are not protected")
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
		connectivity: make(map[string]agentConnectivity),
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go s.startRetentionCleaner(ctx)
	go s.watchConfig(ctx, *configPath, *format)

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
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
		s.hub.closeAll()
		if closer, ok := store.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				log.Printf("close storage: %v", err)
			}
		}
	}()
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func (s *server) currentConfig() config.Config {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

func (s *server) watchConfig(ctx context.Context, path, format string) {
	if strings.TrimSpace(path) == "" {
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		log.Printf("config reload disabled: %v", err)
		return
	}
	lastMod, lastSize := info.ModTime(), info.Size()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(path)
			if err != nil {
				log.Printf("config reload stat: %v", err)
				continue
			}
			if info.ModTime().Equal(lastMod) && info.Size() == lastSize {
				continue
			}
			lastMod, lastSize = info.ModTime(), info.Size()
			loaded, err := config.Load(path, format)
			if err != nil {
				log.Printf("config reload rejected: %v", err)
				continue
			}
			if err := validateSupervisorConfig(loaded); err != nil {
				log.Printf("config reload rejected: %v", err)
				continue
			}
			if s.applyReloadedConfig(loaded) {
				log.Printf("config reloaded: targets=%d schedule=%ds", len(loaded.Targets), loaded.Params.ScheduleSeconds)
			}
		}
	}
}

func validateSupervisorConfig(cfg config.Config) error {
	for i, target := range cfg.Targets {
		if strings.TrimSpace(target.Name) == "" || strings.TrimSpace(target.Address) == "" {
			return fmt.Errorf("target %d requires name and address", i+1)
		}
		if target.Port < 1 || target.Port > 65535 {
			return fmt.Errorf("target %q has invalid port", target.Name)
		}
	}
	for _, value := range cfg.DashboardRanges {
		if _, ok := parseRangeDuration(value); !ok {
			return fmt.Errorf("invalid dashboard range %q", value)
		}
	}
	if _, ok := parseRangeDuration(cfg.DefaultRange); !ok {
		return fmt.Errorf("invalid default range %q", cfg.DefaultRange)
	}
	return nil
}

func (s *server) applyReloadedConfig(loaded config.Config) bool {
	current := s.currentConfig()
	if loaded.Listen != current.Listen || loaded.SQLitePath != current.SQLitePath ||
		loaded.DashboardUser != current.DashboardUser || loaded.DashboardPassword != current.DashboardPassword {
		log.Printf("config reload: listen, sqlite_path and dashboard credentials require restart")
		loaded.Listen = current.Listen
		loaded.SQLitePath = current.SQLitePath
		loaded.DashboardUser = current.DashboardUser
		loaded.DashboardPassword = current.DashboardPassword
	}
	if reflect.DeepEqual(current, loaded) {
		return false
	}
	s.cfgMu.Lock()
	s.cfg = loaded
	s.cfgMu.Unlock()
	if s.resultsCache != nil {
		s.resultsCache.clear()
	}
	if s.hub != nil {
		s.hub.broadcast("refresh")
	}
	return true
}

func newSupervisorMux(s *server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/api/tasks", s.requireAgentAuth(s.handleTasks))
	mux.HandleFunc("/api/report", s.requireAgentAuth(s.handleReport))
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

func (s *server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if pinger, ok := s.store.(interface{ PingContext(context.Context) error }); ok {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := pinger.PingContext(ctx); err != nil {
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
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
	cfg := s.currentConfig()
	tasks := make([]model.Task, 0, len(cfg.Targets))
	for _, target := range cfg.Targets {
		tasks = append(tasks, model.Task{Target: target, Params: cfg.Params})
	}
	writeJSON(w, tasks)
}

func (s *server) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, maxReportBodyBytes)
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
	if len(results) > maxReportBatchSize {
		http.Error(w, "too many report results", http.StatusRequestEntityTooLarge)
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
		cfg := s.currentConfig()
		if !result.CheckedAt.IsZero() && (result.CheckedAt.After(seenAt.Add(10*time.Minute)) || result.CheckedAt.Before(seenAt.AddDate(0, 0, -cfg.RetentionDays))) {
			http.Error(w, "checked_at is outside the accepted range", http.StatusBadRequest)
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
	s.updateAgentConnectivity(saved, seenAt)
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
	if len(result.Agent) > 128 {
		return fmt.Errorf("agent is too long")
	}
	if strings.TrimSpace(result.TargetName) == "" {
		return fmt.Errorf("target_name is required")
	}
	if len(result.TargetName) > 256 {
		return fmt.Errorf("target_name is too long")
	}
	if strings.TrimSpace(result.Address) == "" {
		return fmt.Errorf("address is required")
	}
	if len(result.Address) > 512 {
		return fmt.Errorf("address is too long")
	}
	if len(result.AgentIP) > 256 {
		return fmt.Errorf("agent_ip is too long")
	}
	if len(result.Error) > 2048 {
		return fmt.Errorf("error is too long")
	}
	if len(result.Labels) > 64 {
		return fmt.Errorf("too many labels")
	}
	for _, label := range result.Labels {
		if len(label) > 128 {
			return fmt.Errorf("label is too long")
		}
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
	if result.SuccessCount > 10000 || result.FailureCount > 10000 {
		return fmt.Errorf("sample count is too large")
	}
	if result.AverageLatencyMS < 0 || math.IsNaN(result.AverageLatencyMS) || math.IsInf(result.AverageLatencyMS, 0) {
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

func (s *server) updateAgentConnectivity(results []model.Result, now time.Time) {
	type round struct{ total, failed int }
	rounds := make(map[string]round)
	for _, result := range results {
		r := rounds[result.Agent]
		r.total++
		if result.SuccessCount == 0 {
			r.failed++
		}
		rounds[result.Agent] = r
	}
	threshold := s.currentConfig().FailureThreshold
	if threshold < 1 {
		threshold = 1
	}
	s.connectivityMu.Lock()
	defer s.connectivityMu.Unlock()
	if s.connectivity == nil {
		s.connectivity = make(map[string]agentConnectivity)
	}
	for agent, round := range rounds {
		state := s.connectivity[agent]
		failed := round.failed*2 > round.total
		if failed {
			state.ConsecutiveFailures++
			if state.FirstFailedAt.IsZero() {
				state.FirstFailedAt = now
			}
			if state.ConsecutiveFailures >= threshold {
				if state.Status != "failed" {
					state.Status = "failed"
					state.LastChangedAt = now
					log.Printf("[ALERT] agent=%s connectivity=failed consecutive_rounds=%d", agent, state.ConsecutiveFailures)
				}
			} else if state.Status != "checking" {
				state.Status = "checking"
				state.LastChangedAt = now
			}
		} else {
			if state.Status == "failed" {
				log.Printf("[RECOVERY] agent=%s connectivity=ok", agent)
			}
			if state.Status != "ok" {
				state.LastChangedAt = now
			}
			state.Status = "ok"
			state.ConsecutiveFailures = 0
			state.FirstFailedAt = time.Time{}
		}
		s.connectivity[agent] = state
	}
}

func (s *server) connectivityFor(agent string) agentConnectivity {
	s.connectivityMu.Lock()
	defer s.connectivityMu.Unlock()
	return s.connectivity[agent]
}

func (s *server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if !s.dashboardAuthenticated(r) {
		http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
		return
	}
	selectedRange := s.selectedRange(r)
	cfg := s.currentConfig()
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, max-age=10")
	if err := s.tpl.Execute(w, dashboardData{
		Agent:               agent,
		Ranges:              cfg.DashboardRanges,
		SelectedRange:       selectedRange,
		CustomRange:         !rangeInList(selectedRange, cfg.DashboardRanges),
		OfflineAfterSeconds: s.agentOfflineAfterSeconds(),
		RollupIntervalMins:  cfg.RollupIntervalMins,
		AssetVersion:        embeddedAssetVersion,
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
		connectivity := s.connectivityFor(status.Agent)
		if connectivity.Status == "" {
			connectivity.Status = "unknown"
		}
		if now.Sub(status.LastSeenAt) > time.Duration(offlineAfter)*time.Second {
			state = "offline"
		} else if connectivity.Status == "failed" {
			state = "degraded"
		}
		out = append(out, agentStatusView{
			Agent:                 status.Agent,
			AgentIP:               status.AgentIP,
			FirstSeenAt:           status.FirstSeenAt,
			LastSeenAt:            status.LastSeenAt,
			OfflineAfterSeconds:   offlineAfter,
			Status:                state,
			Connectivity:          connectivity.Status,
			ConsecutiveFailures:   connectivity.ConsecutiveFailures,
			ConnectivityChangedAt: connectivity.LastChangedAt,
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
	s.connectivityMu.Lock()
	delete(s.connectivity, agent)
	s.connectivityMu.Unlock()
	s.resultsCache.clear()
	if s.hub != nil {
		s.hub.broadcast("refresh")
	}
	writeJSON(w, map[string]any{"status": "ok", "agent": agent})
}

func (s *server) agentOfflineAfterSeconds() int {
	cfg := s.currentConfig()
	seconds := cfg.Params.ScheduleSeconds
	if seconds <= 0 {
		seconds = cfg.TaskIntervalSeconds
	}
	if seconds <= 0 {
		seconds = 30
	}
	return seconds * 3
}

func (s *server) selectedRange(r *http.Request) string {
	cfg := s.currentConfig()
	raw := r.URL.Query().Get("range")
	if raw == "" {
		if cookie, err := r.Cookie("pingmon_range"); err == nil {
			raw = cookie.Value
		}
	}
	if raw == "" {
		raw = cfg.DefaultRange
	}
	for _, allowed := range cfg.DashboardRanges {
		if raw == allowed {
			return raw
		}
	}
	if _, ok := parseRangeDuration(raw); ok {
		return strings.TrimSpace(strings.ToLower(raw))
	}
	if len(cfg.DashboardRanges) > 0 {
		return cfg.DashboardRanges[0]
	}
	return "24h"
}

func (s *server) resultsSince(since time.Time, agent string) ([]model.Result, error) {
	rawCutoff := time.Now().AddDate(0, 0, -s.currentConfig().RawRetentionDays)
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
	rawCutoff := time.Now().AddDate(0, 0, -s.currentConfig().RawRetentionDays)
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

func (h *websocketHub) closeAll() {
	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		h.remove(c)
	}
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

func (s *server) requireAgentAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimSpace(s.currentConfig().AgentToken)
		if token == "" {
			next(w, r)
			return
		}
		provided := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="pingmon-agent"`)
			http.Error(w, "agent authentication required", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
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
	cfg := s.currentConfig()
	user, pass, ok := r.BasicAuth()
	if ok &&
		subtle.ConstantTimeCompare([]byte(user), []byte(cfg.DashboardUser)) == 1 &&
		subtle.ConstantTimeCompare([]byte(pass), []byte(cfg.DashboardPassword)) == 1 {
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
	cfg := s.currentConfig()
	if subtle.ConstantTimeCompare([]byte(user), []byte(cfg.DashboardUser)) != 1 ||
		subtle.ConstantTimeCompare([]byte(pass), []byte(cfg.DashboardPassword)) != 1 {
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
	cfg := s.currentConfig()
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
	if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(cfg.DashboardUser)) != 1 {
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

func (s *server) startRetentionCleaner(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	s.cleanOldData()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanOldData()
		}
	}
}

func (s *server) cleanOldData() {
	now := time.Now()
	cfg := s.currentConfig()
	rawCutoff := now.AddDate(0, 0, -cfg.RawRetentionDays)
	retentionCutoff := now.AddDate(0, 0, -cfg.RetentionDays)
	rollupInterval := time.Duration(cfg.RollupIntervalMins) * time.Minute

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
	maxBytes   int
	bytes      int
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
		maxBytes:   resultsCacheMaxBytes,
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
				c.bytes -= len(current.data)
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
	if c.maxBytes > 0 && len(data) > c.maxBytes {
		return
	}
	c.mu.Lock()
	now := time.Now()
	for existingKey, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			c.bytes -= len(entry.data)
			delete(c.entries, existingKey)
		}
	}
	if previous, ok := c.entries[key]; ok {
		c.bytes -= len(previous.data)
		delete(c.entries, key)
	}
	for (c.maxEntries > 0 && len(c.entries) >= c.maxEntries) || (c.maxBytes > 0 && c.bytes+len(data) > c.maxBytes) {
		var oldestKey resultsCacheKey
		var oldestExpiry time.Time
		for existingKey, entry := range c.entries {
			if oldestExpiry.IsZero() || entry.expiresAt.Before(oldestExpiry) {
				oldestKey, oldestExpiry = existingKey, entry.expiresAt
			}
		}
		c.bytes -= len(c.entries[oldestKey].data)
		delete(c.entries, oldestKey)
	}
	c.entries[key] = resultsCacheEntry{
		data:      append([]byte(nil), data...),
		expiresAt: now.Add(c.ttl),
	}
	c.bytes += len(data)
	c.mu.Unlock()
}

func (c *resultsCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = make(map[resultsCacheKey]resultsCacheEntry)
	c.bytes = 0
	c.mu.Unlock()
}
