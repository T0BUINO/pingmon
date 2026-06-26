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
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"
	"pingmon/internal/storage"
)

const (
	maxProblemLogRows = 200
	resultsCacheTTL   = 2 * time.Second
	overviewCacheTTL  = 2 * time.Second
	maxSeriesPoints   = 360
)

type server struct {
	cfg           config.Config
	store         storage.Store
	tpl           *template.Template
	hub           *websocketHub
	resultsCache  *resultsCache
	overviewCache *overviewCache
	dashMem       *dashMemCache
}

type agentStatusView struct {
	Agent               string    `json:"agent"`
	AgentIP             string    `json:"agent_ip,omitempty"`
	FirstSeenAt         time.Time `json:"first_seen_at"`
	LastSeenAt          time.Time `json:"last_seen_at"`
	OfflineAfterSeconds int       `json:"offline_after_seconds"`
	Status              string    `json:"status"`
	CacheStatus         string    `json:"cache_status"`
}

type overviewResponse struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Range       string            `json:"range"`
	RangeLabels []string          `json:"range_labels"`
	Selected    string            `json:"selected_agent,omitempty"`
	Summary     overviewSummary   `json:"summary"`
	Agents      []agentOverview   `json:"agents"`
	Targets     []targetOverview  `json:"targets"`
	Problems    []problemOverview `json:"problems"`
	Series      []seriesPoint     `json:"series"`
	Meta        overviewMeta      `json:"meta"`
}

type overviewSummary struct {
	AgentsOnline   int     `json:"agents_online"`
	AgentsTotal    int     `json:"agents_total"`
	Targets        int     `json:"targets"`
	SuccessRate    float64 `json:"success_rate"`
	AverageLatency float64 `json:"average_latency_ms"`
	Problems       int     `json:"problems"`
	Samples        int     `json:"samples"`
}

type agentOverview struct {
	Agent          string    `json:"agent"`
	AgentIP        string    `json:"agent_ip,omitempty"`
	Status         string    `json:"status"`
	FirstSeenAt    time.Time `json:"first_seen_at"`
	LastSeenAt     time.Time `json:"last_seen_at"`
	TargetCount    int       `json:"target_count"`
	Samples        int       `json:"samples"`
	Problems       int       `json:"problems"`
	SuccessRate    float64   `json:"success_rate"`
	AverageLatency float64   `json:"average_latency_ms"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type targetOverview struct {
	Key            string    `json:"key"`
	Agent          string    `json:"agent"`
	TargetName     string    `json:"target_name"`
	Address        string    `json:"address"`
	Port           int       `json:"port"`
	Labels         []string  `json:"labels,omitempty"`
	Samples        int       `json:"samples"`
	Problems       int       `json:"problems"`
	SuccessRate    float64   `json:"success_rate"`
	AverageLatency float64   `json:"average_latency_ms"`
	LastCheckedAt  time.Time `json:"last_checked_at"`
	LastError      string    `json:"last_error,omitempty"`
}

type problemOverview struct {
	CheckedAt      time.Time `json:"checked_at"`
	Agent          string    `json:"agent"`
	AgentIP        string    `json:"agent_ip,omitempty"`
	TargetName     string    `json:"target_name"`
	Address        string    `json:"address"`
	Port           int       `json:"port"`
	Severity       string    `json:"severity"`
	SuccessRate    float64   `json:"success_rate"`
	AverageLatency float64   `json:"average_latency_ms"`
	Error          string    `json:"error,omitempty"`
}

type seriesPoint struct {
	Timestamp      time.Time `json:"timestamp"`
	Agent          string    `json:"agent,omitempty"`
	TargetName     string    `json:"target_name,omitempty"`
	SuccessRate    float64   `json:"success_rate"`
	AverageLatency float64   `json:"average_latency_ms"`
	Samples        int       `json:"samples"`
	Problems       int       `json:"problems"`
}

type overviewMeta struct {
	OfflineAfterSeconds int `json:"offline_after_seconds"`
	RollupIntervalMins  int `json:"rollup_interval_minutes"`
	RawRetentionDays    int `json:"raw_retention_days"`
}

type websocketHub struct {
	mu      sync.Mutex
	clients map[*wsClient]struct{}
}

type wsClient struct {
	conn net.Conn
	send chan []byte
	done chan struct{}
}

type websocketEvent struct {
	Type    string         `json:"type"`
	Results []model.Result `json:"results,omitempty"`
}

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
	s := &server{
		cfg:           cfg,
		store:         store,
		tpl:           template.Must(template.New("dashboard").Parse(dashboardHTML)),
		hub:           newWebsocketHub(),
		resultsCache:  newResultsCache(resultsCacheTTL),
		overviewCache: newOverviewCache(overviewCacheTTL),
		dashMem:       newDashMemCache(),
	}
	go s.startRetentionCleaner()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	})
	mux.HandleFunc("/dashboard", s.handleDashboard)
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/report", s.handleReport)
	mux.HandleFunc("/api/overview", s.requireDashboardAuth(s.handleOverview))
	mux.HandleFunc("/api/agents", s.requireDashboardAuth(s.handleAgents))
	mux.HandleFunc("/api/results", s.requireDashboardAuth(s.handleResults))
	mux.HandleFunc("/api/cache-status", s.requireDashboardAuth(s.handleCacheStatus))
	mux.HandleFunc("/ws", s.requireDashboardAuth(s.handleWebSocket))

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
	validated := make([]model.Result, 0, len(results))
	for _, result := range results {
		if err := validateReportResult(result); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		normalizeReport(&result, agentIP)
		validated = append(validated, result)
	}
	saved, err := s.store.SaveResults(validated, seenAt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.invalidateDataCaches(saved)
	s.checkFailureAlerts(saved)
	writeJSON(w, map[string]any{"status": "ok", "saved": len(saved)})
}

func (s *server) invalidateDataCaches(saved []model.Result) {
	if s.resultsCache != nil {
		s.resultsCache.clear()
	}
	if s.overviewCache != nil {
		s.overviewCache.clear()
	}
	if s.dashMem != nil {
		seen := make(map[string]bool)
		for _, result := range saved {
			if seen[result.Agent] {
				continue
			}
			seen[result.Agent] = true
			s.dashMem.invalidate(result.Agent)
		}
	}
	if s.hub != nil {
		s.hub.broadcastJSON(websocketEvent{Type: "results", Results: saved})
	}
}

func (s *server) checkFailureAlerts(results []model.Result) {
	limit := s.cfg.FailureThreshold + 1
	seenTargets := make(map[string]bool, len(results))
	for _, result := range results {
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
}

func (s *server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if !s.checkDashboardAuth(w, r) {
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, max-age=30")
	data := struct {
		Ranges        []string
		DefaultRange  string
		SelectedAgent string
	}{
		Ranges:        s.cfg.DashboardRanges,
		DefaultRange:  s.selectedRange(r),
		SelectedAgent: strings.TrimSpace(r.URL.Query().Get("agent")),
	}
	if err := s.tpl.Execute(w, data); err != nil {
		log.Printf("render dashboard: %v", err)
	}
}

func (s *server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	selectedRange := s.selectedRange(r)
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	cacheKey := overviewCacheKey{selectedRange: selectedRange, agent: agent}
	if data, ok := s.overviewCache.get(cacheKey); ok {
		writeJSONBytes(w, data)
		return
	}
	overview, err := s.buildOverview(selectedRange, agent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(overview)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.overviewCache.set(cacheKey, data)
	writeJSONBytes(w, data)
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

func (s *server) writeDashboardResults(w http.ResponseWriter, selectedRange, agent string) {
	since := time.Now().Add(-rangeDuration(selectedRange))
	rawCutoff := time.Now().AddDate(0, 0, -s.cfg.RawRetentionDays)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, max-age=5")

	if !since.Before(rawCutoff) && agent != "" && s.dashMem != nil {
		rows := s.dashMem.get(agent)
		if rows == nil {
			cacheSince := time.Now().AddDate(0, 0, -s.cfg.RawRetentionDays)
			rows = s.buildCache(agent, cacheSince)
		}
		if len(rows) > 0 {
			serveFromCache(w, rows, since)
			return
		}
	}
	s.serveStreamDirect(w, since, agent)
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
	writeJSON(w, s.agentStatusViews(statuses))
}

func (s *server) handleCacheStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	agent := strings.TrimSpace(r.URL.Query().Get("agent"))
	writeJSON(w, map[string]string{"agent": agent, "cache_status": "ready"})
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
	if s.resultsCache != nil {
		s.resultsCache.clear()
	}
	if s.overviewCache != nil {
		s.overviewCache.clear()
	}
	if s.dashMem != nil {
		s.dashMem.invalidate(agent)
	}
	if s.hub != nil {
		s.hub.broadcast("refresh")
	}
	writeJSON(w, map[string]any{"status": "ok", "agent": agent})
}

func (s *server) buildOverview(selectedRange, selectedAgent string) (overviewResponse, error) {
	since := time.Now().Add(-rangeDuration(selectedRange))
	statuses, err := s.store.ListAgentStatuses()
	if err != nil {
		return overviewResponse{}, err
	}
	overview := overviewResponse{
		GeneratedAt: time.Now(),
		Range:       selectedRange,
		RangeLabels: append([]string(nil), s.cfg.DashboardRanges...),
		Selected:    selectedAgent,
		Meta: overviewMeta{
			OfflineAfterSeconds: s.agentOfflineAfterSeconds(),
			RollupIntervalMins:  s.cfg.RollupIntervalMins,
			RawRetentionDays:    s.cfg.RawRetentionDays,
		},
	}
	views := s.agentStatusViews(statuses)
	statusByAgent := make(map[string]agentStatusView, len(views))
	for _, status := range views {
		statusByAgent[status.Agent] = status
		if selectedAgent == "" || status.Agent == selectedAgent {
			overview.Agents = append(overview.Agents, agentOverview{
				Agent:       status.Agent,
				AgentIP:     status.AgentIP,
				Status:      status.Status,
				FirstSeenAt: status.FirstSeenAt,
				LastSeenAt:  status.LastSeenAt,
				UpdatedAt:   status.LastSeenAt,
			})
		}
	}
	builder := newOverviewBuilder(&overview, statusByAgent, selectedAgent, rangeDuration(selectedRange))
	if err := s.streamResultsSince(since, selectedAgent, builder.add); err != nil {
		return overviewResponse{}, err
	}
	builder.finish()
	sort.Slice(overview.Agents, func(i, j int) bool {
		if overview.Agents[i].Status != overview.Agents[j].Status {
			return overview.Agents[i].Status == "online"
		}
		return overview.Agents[i].UpdatedAt.After(overview.Agents[j].UpdatedAt)
	})
	sort.Slice(overview.Targets, func(i, j int) bool {
		if overview.Targets[i].Problems != overview.Targets[j].Problems {
			return overview.Targets[i].Problems > overview.Targets[j].Problems
		}
		return overview.Targets[i].LastCheckedAt.After(overview.Targets[j].LastCheckedAt)
	})
	return overview, nil
}

type overviewBuilder struct {
	overview       *overviewResponse
	statuses       map[string]agentStatusView
	selectedAgent  string
	agentAggs      map[string]*metricAgg
	targetAggs     map[string]*metricAgg
	targetRows     map[string]model.Result
	targetLabels   map[string][]string
	targetProblems map[string]int
	agentTargets   map[string]map[string]bool
	problems       []problemOverview
	all            metricAgg
	series         map[string]*metricAgg
	seriesTimes    map[string]time.Time
	seriesBucket   time.Duration
}

func newOverviewBuilder(overview *overviewResponse, statuses map[string]agentStatusView, selectedAgent string, selectedDuration time.Duration) *overviewBuilder {
	bucket := selectedDuration / maxSeriesPoints
	if bucket < time.Minute {
		bucket = time.Minute
	}
	return &overviewBuilder{
		overview:       overview,
		statuses:       statuses,
		selectedAgent:  selectedAgent,
		agentAggs:      make(map[string]*metricAgg),
		targetAggs:     make(map[string]*metricAgg),
		targetRows:     make(map[string]model.Result),
		targetLabels:   make(map[string][]string),
		targetProblems: make(map[string]int),
		agentTargets:   make(map[string]map[string]bool),
		problems:       make([]problemOverview, 0, maxProblemLogRows),
		series:         make(map[string]*metricAgg),
		seriesTimes:    make(map[string]time.Time),
		seriesBucket:   bucket,
	}
}

func (b *overviewBuilder) add(row model.Result) error {
	if b.selectedAgent != "" && row.Agent != b.selectedAgent {
		return nil
	}
	b.all.add(row)
	agentAgg := b.agentAggs[row.Agent]
	if agentAgg == nil {
		agentAgg = &metricAgg{}
		b.agentAggs[row.Agent] = agentAgg
	}
	agentAgg.add(row)

	targetKey := row.Agent + "\x00" + row.TargetName + "\x00" + row.Address + "\x00" + strconv.Itoa(row.Port)
	targetAgg := b.targetAggs[targetKey]
	if targetAgg == nil {
		targetAgg = &metricAgg{}
		b.targetAggs[targetKey] = targetAgg
	}
	targetAgg.add(row)
	if existing, ok := b.targetRows[targetKey]; !ok || row.CheckedAt.After(existing.CheckedAt) {
		b.targetRows[targetKey] = row
		b.targetLabels[targetKey] = append([]string(nil), row.Labels...)
	}
	if b.agentTargets[row.Agent] == nil {
		b.agentTargets[row.Agent] = make(map[string]bool)
	}
	b.agentTargets[row.Agent][targetKey] = true

	if isProblemResult(row) {
		b.targetProblems[targetKey]++
		b.rememberProblem(row)
	}
	b.addSeries(row)
	return nil
}

func (b *overviewBuilder) rememberProblem(row model.Result) {
	problem := problemOverview{
		CheckedAt:      row.CheckedAt,
		Agent:          row.Agent,
		AgentIP:        row.AgentIP,
		TargetName:     row.TargetName,
		Address:        row.Address,
		Port:           row.Port,
		Severity:       resultSeverity(row),
		SuccessRate:    row.SuccessRate,
		AverageLatency: row.AverageLatencyMS,
		Error:          row.Error,
	}
	if len(b.problems) < maxProblemLogRows {
		b.problems = append(b.problems, problem)
		return
	}
	oldest := 0
	for i := 1; i < len(b.problems); i++ {
		if b.problems[i].CheckedAt.Before(b.problems[oldest].CheckedAt) {
			oldest = i
		}
	}
	if problem.CheckedAt.After(b.problems[oldest].CheckedAt) {
		b.problems[oldest] = problem
	}
}

func (b *overviewBuilder) addSeries(row model.Result) {
	t := row.CheckedAt.UTC().Truncate(b.seriesBucket)
	label := row.Agent
	if b.selectedAgent != "" {
		label = row.TargetName
	}
	key := t.Format(time.RFC3339Nano) + "\x00" + label
	agg := b.series[key]
	if agg == nil {
		agg = &metricAgg{}
		b.series[key] = agg
		b.seriesTimes[key] = t
	}
	agg.add(row)
}

func (b *overviewBuilder) finish() {
	b.finishAgents()
	b.finishTargets()
	b.finishProblems()
	b.finishSeries()
	b.overview.Summary = overviewSummary{
		AgentsTotal:    len(b.overview.Agents),
		Targets:        len(b.overview.Targets),
		SuccessRate:    b.all.successRate(),
		AverageLatency: b.all.averageLatency(),
		Problems:       b.all.problems,
		Samples:        b.all.samples,
	}
	for _, agent := range b.overview.Agents {
		if agent.Status == "online" {
			b.overview.Summary.AgentsOnline++
		}
	}
}

func (b *overviewBuilder) finishAgents() {
	agentIndex := make(map[string]int, len(b.overview.Agents))
	for i := range b.overview.Agents {
		agentIndex[b.overview.Agents[i].Agent] = i
	}
	for agent, agg := range b.agentAggs {
		status := b.statuses[agent]
		next := agentOverview{
			Agent:          agent,
			AgentIP:        status.AgentIP,
			Status:         status.Status,
			FirstSeenAt:    status.FirstSeenAt,
			LastSeenAt:     status.LastSeenAt,
			TargetCount:    len(b.agentTargets[agent]),
			Samples:        agg.samples,
			Problems:       agg.problems,
			SuccessRate:    agg.successRate(),
			AverageLatency: agg.averageLatency(),
			UpdatedAt:      agg.last,
		}
		if next.AgentIP == "" {
			next.AgentIP = agg.agentIP
		}
		if next.Status == "" {
			next.Status = "unknown"
		}
		if i, ok := agentIndex[agent]; ok {
			if b.overview.Agents[i].AgentIP == "" {
				b.overview.Agents[i].AgentIP = next.AgentIP
			}
			b.overview.Agents[i].TargetCount = next.TargetCount
			b.overview.Agents[i].Samples = next.Samples
			b.overview.Agents[i].Problems = next.Problems
			b.overview.Agents[i].SuccessRate = next.SuccessRate
			b.overview.Agents[i].AverageLatency = next.AverageLatency
			if next.UpdatedAt.After(b.overview.Agents[i].UpdatedAt) {
				b.overview.Agents[i].UpdatedAt = next.UpdatedAt
			}
		} else {
			b.overview.Agents = append(b.overview.Agents, next)
		}
	}
}

func (b *overviewBuilder) finishTargets() {
	for key, agg := range b.targetAggs {
		row := b.targetRows[key]
		b.overview.Targets = append(b.overview.Targets, targetOverview{
			Key:            key,
			Agent:          row.Agent,
			TargetName:     row.TargetName,
			Address:        row.Address,
			Port:           row.Port,
			Labels:         b.targetLabels[key],
			Samples:        agg.samples,
			Problems:       b.targetProblems[key],
			SuccessRate:    agg.successRate(),
			AverageLatency: agg.averageLatency(),
			LastCheckedAt:  row.CheckedAt,
			LastError:      row.Error,
		})
	}
}

func (b *overviewBuilder) finishProblems() {
	sort.Slice(b.problems, func(i, j int) bool {
		return b.problems[i].CheckedAt.After(b.problems[j].CheckedAt)
	})
	b.overview.Problems = b.problems
}

func (b *overviewBuilder) finishSeries() {
	points := make([]seriesPoint, 0, len(b.series))
	for key, agg := range b.series {
		parts := strings.Split(key, "\x00")
		point := seriesPoint{
			Timestamp:      b.seriesTimes[key],
			SuccessRate:    agg.successRate(),
			AverageLatency: agg.averageLatency(),
			Samples:        agg.samples,
			Problems:       agg.problems,
		}
		if b.selectedAgent != "" {
			point.TargetName = parts[1]
		} else {
			point.Agent = parts[1]
		}
		points = append(points, point)
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].Timestamp.Equal(points[j].Timestamp) {
			return points[i].Agent+points[i].TargetName < points[j].Agent+points[j].TargetName
		}
		return points[i].Timestamp.Before(points[j].Timestamp)
	})
	b.overview.Series = points
}

func (o *overviewResponse) applyRows(rows []model.Result, statuses map[string]agentStatusView, selectedAgent string) {
	agentAggs := make(map[string]*metricAgg)
	targetAggs := make(map[string]*metricAgg)
	targetRows := make(map[string]model.Result)
	targetLabels := make(map[string][]string)
	targetProblems := make(map[string]int)
	agentTargets := make(map[string]map[string]bool)
	problems := make([]problemOverview, 0)
	var all metricAgg

	for _, row := range rows {
		if selectedAgent != "" && row.Agent != selectedAgent {
			continue
		}
		all.add(row)
		agentAgg := agentAggs[row.Agent]
		if agentAgg == nil {
			agentAgg = &metricAgg{}
			agentAggs[row.Agent] = agentAgg
		}
		agentAgg.add(row)
		targetKey := row.Agent + "\x00" + row.TargetName + "\x00" + row.Address + "\x00" + strconv.Itoa(row.Port)
		targetAgg := targetAggs[targetKey]
		if targetAgg == nil {
			targetAgg = &metricAgg{}
			targetAggs[targetKey] = targetAgg
		}
		targetAgg.add(row)
		if existing, ok := targetRows[targetKey]; !ok || row.CheckedAt.After(existing.CheckedAt) {
			targetRows[targetKey] = row
			targetLabels[targetKey] = append([]string(nil), row.Labels...)
		}
		if agentTargets[row.Agent] == nil {
			agentTargets[row.Agent] = make(map[string]bool)
		}
		agentTargets[row.Agent][targetKey] = true
		if isProblemResult(row) {
			targetProblems[targetKey]++
			problems = append(problems, problemOverview{
				CheckedAt:      row.CheckedAt,
				Agent:          row.Agent,
				AgentIP:        row.AgentIP,
				TargetName:     row.TargetName,
				Address:        row.Address,
				Port:           row.Port,
				Severity:       resultSeverity(row),
				SuccessRate:    row.SuccessRate,
				AverageLatency: row.AverageLatencyMS,
				Error:          row.Error,
			})
		}
	}

	agentIndex := make(map[string]int, len(o.Agents))
	for i := range o.Agents {
		agentIndex[o.Agents[i].Agent] = i
	}
	for agent, agg := range agentAggs {
		status := statuses[agent]
		next := agentOverview{
			Agent:          agent,
			AgentIP:        status.AgentIP,
			Status:         status.Status,
			FirstSeenAt:    status.FirstSeenAt,
			LastSeenAt:     status.LastSeenAt,
			TargetCount:    len(agentTargets[agent]),
			Samples:        agg.samples,
			Problems:       agg.problems,
			SuccessRate:    agg.successRate(),
			AverageLatency: agg.averageLatency(),
			UpdatedAt:      agg.last,
		}
		if next.AgentIP == "" {
			next.AgentIP = agg.agentIP
		}
		if next.Status == "" {
			next.Status = "unknown"
		}
		if i, ok := agentIndex[agent]; ok {
			if o.Agents[i].AgentIP == "" {
				o.Agents[i].AgentIP = next.AgentIP
			}
			o.Agents[i].TargetCount = next.TargetCount
			o.Agents[i].Samples = next.Samples
			o.Agents[i].Problems = next.Problems
			o.Agents[i].SuccessRate = next.SuccessRate
			o.Agents[i].AverageLatency = next.AverageLatency
			if next.UpdatedAt.After(o.Agents[i].UpdatedAt) {
				o.Agents[i].UpdatedAt = next.UpdatedAt
			}
		} else {
			o.Agents = append(o.Agents, next)
		}
	}
	for key, agg := range targetAggs {
		row := targetRows[key]
		o.Targets = append(o.Targets, targetOverview{
			Key:            key,
			Agent:          row.Agent,
			TargetName:     row.TargetName,
			Address:        row.Address,
			Port:           row.Port,
			Labels:         targetLabels[key],
			Samples:        agg.samples,
			Problems:       targetProblems[key],
			SuccessRate:    agg.successRate(),
			AverageLatency: agg.averageLatency(),
			LastCheckedAt:  row.CheckedAt,
			LastError:      row.Error,
		})
	}
	sort.Slice(problems, func(i, j int) bool {
		return problems[i].CheckedAt.After(problems[j].CheckedAt)
	})
	if len(problems) > maxProblemLogRows {
		problems = problems[:maxProblemLogRows]
	}
	o.Problems = problems
	o.Series = buildSeries(rows, selectedAgent)
	o.Summary = overviewSummary{
		AgentsTotal:    len(o.Agents),
		Targets:        len(o.Targets),
		SuccessRate:    all.successRate(),
		AverageLatency: all.averageLatency(),
		Problems:       all.problems,
		Samples:        all.samples,
	}
	for _, agent := range o.Agents {
		if agent.Status == "online" {
			o.Summary.AgentsOnline++
		}
	}
}

type metricAgg struct {
	samples         int
	problems        int
	successCount    int
	failureCount    int
	latencyWeighted float64
	latencySamples  int
	last            time.Time
	agentIP         string
}

func (a *metricAgg) add(row model.Result) {
	a.samples++
	a.successCount += row.SuccessCount
	a.failureCount += row.FailureCount
	if row.SuccessCount > 0 {
		a.latencyWeighted += row.AverageLatencyMS * float64(row.SuccessCount)
		a.latencySamples += row.SuccessCount
	}
	if isProblemResult(row) {
		a.problems++
	}
	if row.CheckedAt.After(a.last) {
		a.last = row.CheckedAt
	}
	if a.agentIP == "" && row.AgentIP != "" {
		a.agentIP = row.AgentIP
	}
}

func (a metricAgg) successRate() float64 {
	total := a.successCount + a.failureCount
	if total == 0 {
		return 0
	}
	return float64(a.successCount) / float64(total)
}

func (a metricAgg) averageLatency() float64 {
	if a.latencySamples == 0 {
		return 0
	}
	return a.latencyWeighted / float64(a.latencySamples)
}

func buildSeries(rows []model.Result, selectedAgent string) []seriesPoint {
	if len(rows) == 0 {
		return nil
	}
	minTime, maxTime := rows[0].CheckedAt, rows[0].CheckedAt
	for _, row := range rows[1:] {
		if row.CheckedAt.Before(minTime) {
			minTime = row.CheckedAt
		}
		if row.CheckedAt.After(maxTime) {
			maxTime = row.CheckedAt
		}
	}
	span := maxTime.Sub(minTime)
	bucket := time.Minute
	if span > 0 {
		bucket = time.Duration(math.Ceil(float64(span) / float64(maxSeriesPoints)))
	}
	if bucket < time.Minute {
		bucket = time.Minute
	}
	buckets := make(map[string]*metricAgg)
	for _, row := range rows {
		if selectedAgent != "" && row.Agent != selectedAgent {
			continue
		}
		t := row.CheckedAt.UTC().Truncate(bucket)
		key := t.Format(time.RFC3339Nano)
		if selectedAgent != "" {
			key += "\x00" + row.TargetName
		} else {
			key += "\x00" + row.Agent
		}
		agg := buckets[key]
		if agg == nil {
			agg = &metricAgg{last: t}
			buckets[key] = agg
		}
		agg.add(row)
	}
	points := make([]seriesPoint, 0, len(buckets))
	for key, agg := range buckets {
		parts := strings.Split(key, "\x00")
		t, _ := time.Parse(time.RFC3339Nano, parts[0])
		point := seriesPoint{
			Timestamp:      t,
			SuccessRate:    agg.successRate(),
			AverageLatency: agg.averageLatency(),
			Samples:        agg.samples,
			Problems:       agg.problems,
		}
		if selectedAgent != "" {
			point.TargetName = parts[1]
		} else {
			point.Agent = parts[1]
		}
		points = append(points, point)
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].Timestamp.Equal(points[j].Timestamp) {
			return points[i].Agent+points[i].TargetName < points[j].Agent+points[j].TargetName
		}
		return points[i].Timestamp.Before(points[j].Timestamp)
	})
	return points
}

func (s *server) agentStatusViews(statuses []model.AgentStatus) []agentStatusView {
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
			CacheStatus:         "ready",
		})
	}
	return out
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
	valueText := raw[:len(raw)-1]
	multiplier := time.Hour
	if strings.HasSuffix(raw, "mo") {
		valueText = strings.TrimSuffix(raw, "mo")
		multiplier = 30 * 24 * time.Hour
	} else {
		switch raw[len(raw)-1:] {
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
	c := s.hub.add(conn)
	go func() {
		defer s.hub.remove(c)
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
	return &websocketHub{clients: make(map[*wsClient]struct{})}
}

func (h *websocketHub) add(conn net.Conn) *wsClient {
	c := &wsClient{conn: conn, send: make(chan []byte, 16), done: make(chan struct{})}
	c.send <- buildWebSocketFrame("connected")
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
	for {
		select {
		case <-c.done:
			return
		case frame, ok := <-c.send:
			if !ok {
				return
			}
			if _, err := c.conn.Write(frame); err != nil {
				h.remove(c)
				return
			}
		}
	}
}

func (h *websocketHub) broadcast(message string) {
	frame := buildWebSocketFrame(message)
	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		select {
		case <-c.done:
		case c.send <- frame:
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
	h.broadcast(string(payload))
}

func buildWebSocketFrame(message string) []byte {
	payload := []byte(message)
	var frame []byte
	switch {
	case len(payload) < 126:
		frame = make([]byte, 0, 2+len(payload))
		frame = append(frame, 0x81, byte(len(payload)))
	case len(payload) <= 65535:
		frame = make([]byte, 0, 4+len(payload))
		frame = append(frame, 0x81, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		frame = make([]byte, 0, 10+len(payload))
		frame = append(frame, 0x81, 127)
		n := uint64(len(payload))
		for i := 7; i >= 0; i-- {
			frame = append(frame, byte(n>>(8*i)))
		}
	}
	frame = append(frame, payload...)
	return frame
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
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="PingMon Dashboard"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

func (s *server) validDashboardCookie(value string) bool {
	return false
}

func (s *server) startRetentionCleaner() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		s.cleanOldData()
		<-ticker.C
	}
}

func (s *server) cleanOldData() {
	now := time.Now()
	rawCutoff := now.AddDate(0, 0, -s.cfg.RawRetentionDays)
	if n, err := s.store.RollupBefore(rawCutoff, time.Duration(s.cfg.RollupIntervalMins)*time.Minute); err != nil {
		log.Printf("rollup old data: %v", err)
	} else if n > 0 {
		log.Printf("rolled up %d old result buckets", n)
	}
	if n, err := s.store.DeleteBefore(rawCutoff); err != nil {
		log.Printf("delete raw data: %v", err)
	} else if n > 0 {
		log.Printf("deleted %d raw result rows", n)
	}
	rollupCutoff := now.AddDate(0, 0, -s.cfg.RetentionDays)
	if n, err := s.store.DeleteRollupsBefore(rollupCutoff); err != nil {
		log.Printf("delete rollups: %v", err)
	} else if n > 0 {
		log.Printf("deleted %d rollup rows", n)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONBytes(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

type resultsCacheKey struct {
	selectedRange string
	agent         string
}

type resultsCacheEntry struct {
	expires time.Time
	data    []byte
}

type resultsCache struct {
	mu   sync.Mutex
	ttl  time.Duration
	data map[resultsCacheKey]resultsCacheEntry
}

func newResultsCache(ttl time.Duration) *resultsCache {
	return &resultsCache{ttl: ttl, data: make(map[resultsCacheKey]resultsCacheEntry)}
}

func (c *resultsCache) get(key resultsCacheKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	if !ok || time.Now().After(entry.expires) {
		delete(c.data, key)
		return nil, false
	}
	return append([]byte(nil), entry.data...), true
}

func (c *resultsCache) set(key resultsCacheKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = resultsCacheEntry{expires: time.Now().Add(c.ttl), data: append([]byte(nil), data...)}
}

func (c *resultsCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[resultsCacheKey]resultsCacheEntry)
}

type overviewCacheKey struct {
	selectedRange string
	agent         string
}

type overviewCache struct {
	mu   sync.Mutex
	ttl  time.Duration
	data map[overviewCacheKey]resultsCacheEntry
}

func newOverviewCache(ttl time.Duration) *overviewCache {
	return &overviewCache{ttl: ttl, data: make(map[overviewCacheKey]resultsCacheEntry)}
}

func (c *overviewCache) get(key overviewCacheKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	if !ok || time.Now().After(entry.expires) {
		delete(c.data, key)
		return nil, false
	}
	return append([]byte(nil), entry.data...), true
}

func (c *overviewCache) set(key overviewCacheKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = resultsCacheEntry{expires: time.Now().Add(c.ttl), data: append([]byte(nil), data...)}
}

func (c *overviewCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[overviewCacheKey]resultsCacheEntry)
}
