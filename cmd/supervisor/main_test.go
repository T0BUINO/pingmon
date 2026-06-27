package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"pingmon/internal/config"
	"pingmon/internal/model"

	"github.com/gorilla/websocket"
)

type heartbeatCall struct {
	agent   string
	agentIP string
	seenAt  time.Time
}

func TestDashboardTemplateParsesWithoutLegacyHelpers(t *testing.T) {
	if _, err := template.New("dashboard").Parse(dashboardHTML); err != nil {
		t.Fatalf("parse dashboard template: %v", err)
	}
}

func TestDashboardSessionIsSignedAndExpires(t *testing.T) {
	s := &server{
		cfg:     config.Config{DashboardUser: "admin", DashboardPassword: "secret"},
		authKey: []byte("01234567890123456789012345678901"),
	}
	token := s.dashboardSession("admin", time.Now().Add(time.Hour))
	if strings.Contains(token, base64.StdEncoding.EncodeToString([]byte("admin:secret"))) {
		t.Fatal("session token contains encoded credentials")
	}
	if !s.validDashboardCookie(token) {
		t.Fatal("fresh session token was rejected")
	}
	if s.validDashboardCookie(token + "tampered") {
		t.Fatal("tampered session token was accepted")
	}
	if s.validDashboardCookie(s.dashboardSession("admin", time.Now().Add(-time.Second))) {
		t.Fatal("expired session token was accepted")
	}
}

func TestDashboardLoginFlowAvoidsBasicAuthPrompt(t *testing.T) {
	s := &server{
		cfg:     config.DefaultConfig(),
		authKey: []byte("01234567890123456789012345678901"),
		tpl:     template.Must(template.New("dashboard").Parse(dashboardHTML)),
	}

	dashboardRequest := httptest.NewRequest(http.MethodGet, "/dashboard?range=24h", nil)
	dashboardResponse := httptest.NewRecorder()
	s.handleDashboard(dashboardResponse, dashboardRequest)
	if dashboardResponse.Code != http.StatusSeeOther {
		t.Fatalf("dashboard status = %d, want redirect", dashboardResponse.Code)
	}
	if got := dashboardResponse.Header().Get("WWW-Authenticate"); got != "" {
		t.Fatalf("unexpected browser auth challenge %q", got)
	}

	loginRequest := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader("username=admin&password=admin&next=%2Fdashboard"))
	loginRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginResponse := httptest.NewRecorder()
	s.handleLogin(loginResponse, loginRequest)
	if loginResponse.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d, want redirect: %s", loginResponse.Code, loginResponse.Body.String())
	}
	cookies := loginResponse.Result().Cookies()
	if len(cookies) != 1 || !s.validDashboardCookie(cookies[0].Value) {
		t.Fatalf("login cookie is missing or invalid: %+v", cookies)
	}

	authenticatedRequest := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	authenticatedRequest.AddCookie(cookies[0])
	authenticatedResponse := httptest.NewRecorder()
	s.handleDashboard(authenticatedResponse, authenticatedRequest)
	if authenticatedResponse.Code != http.StatusOK {
		t.Fatalf("authenticated dashboard status = %d", authenticatedResponse.Code)
	}
}

func TestDashboardAssetsAreSeparated(t *testing.T) {
	if strings.Contains(dashboardHTML, "<style>") || strings.Contains(dashboardHTML, "<script>") {
		t.Fatal("dashboard template still contains inline CSS or JavaScript")
	}
	if !strings.Contains(dashboardHTML, "/assets/dashboard.css") || !strings.Contains(dashboardHTML, "/assets/dashboard.js") {
		t.Fatal("dashboard template does not reference separated assets")
	}
	if strings.TrimSpace(dashboardCSS) == "" || strings.TrimSpace(dashboardJS) == "" {
		t.Fatal("embedded dashboard assets are empty")
	}
}

func TestMiniChartTouchGesturePolicy(t *testing.T) {
	if !strings.Contains(dashboardCSS, ".mini-chart.chart-surface { height: 86px; touch-action: pan-y; }") {
		t.Fatal("mini chart does not preserve vertical scrolling while reserving horizontal gestures")
	}
	if strings.Contains(dashboardCSS, ".mini-chart.chart-surface { height: 86px; touch-action: none;") {
		t.Fatal("mini chart blocks vertical page scrolling")
	}
	if !strings.Contains(dashboardJS, "addEventListener('pointercancel', () => hideChartTooltip(this))") {
		t.Fatal("chart does not clear tooltip state after a cancelled pointer gesture")
	}
}

func TestWebSocketBroadcast(t *testing.T) {
	s := &server{
		cfg:     config.Config{DashboardUser: "admin", DashboardPassword: "secret"},
		authKey: []byte("01234567890123456789012345678901"),
		hub:     newWebsocketHub(),
	}
	httpServer := httptest.NewServer(gzipHandler(s.requireDashboardAuth(s.handleWebSocket)))
	defer httpServer.Close()

	header := http.Header{}
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("admin:secret")))
	header.Set("Accept-Encoding", "gzip")
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), header)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	_, connected, err := conn.ReadMessage()
	if err != nil || string(connected) != "connected" {
		t.Fatalf("initial websocket message = %q, %v", connected, err)
	}
	s.hub.broadcast("refresh")
	_, message, err := conn.ReadMessage()
	if err != nil || string(message) != "refresh" {
		t.Fatalf("broadcast websocket message = %q, %v", message, err)
	}
}

type fakeStore struct {
	heartbeats        []heartbeatCall
	results           []model.Result
	streamedResults   []model.Result
	deleted           []string
	resultsSinceCalls int
}

func (s *fakeStore) SaveAgentHeartbeat(agent, agentIP string, seenAt time.Time) error {
	s.heartbeats = append(s.heartbeats, heartbeatCall{agent: agent, agentIP: agentIP, seenAt: seenAt})
	return nil
}

func (s *fakeStore) ListAgentStatuses() ([]model.AgentStatus, error) {
	return nil, nil
}

func (s *fakeStore) DeleteAgent(agent string) error {
	s.deleted = append(s.deleted, agent)
	return nil
}

func (s *fakeStore) SaveResults(results []model.Result, seenAt time.Time) ([]model.Result, error) {
	seen := make(map[string]bool, len(results))
	for _, result := range results {
		s.results = append(s.results, result)
		if !seen[result.Agent] {
			seen[result.Agent] = true
			s.heartbeats = append(s.heartbeats, heartbeatCall{agent: result.Agent, agentIP: result.AgentIP, seenAt: seenAt})
		}
	}
	return results, nil
}

func TestHandleDeleteAgentDeletesAgentData(t *testing.T) {
	store := &fakeStore{}
	s := &server{cfg: config.DefaultConfig(), store: store, hub: newWebsocketHub(), resultsCache: newResultsCache(time.Minute, 16)}
	req := httptest.NewRequest(http.MethodDelete, "/api/agents?agent=agent-1", nil)
	rr := httptest.NewRecorder()

	s.handleAgents(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if len(store.deleted) != 1 || store.deleted[0] != "agent-1" {
		t.Fatalf("deleted = %+v, want agent-1", store.deleted)
	}
}

func (s *fakeStore) ResultsSince(since time.Time) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) ResultsSinceForAgent(since time.Time, agent string) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) ResultsSinceCompacted(since, rawCutoff time.Time) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) ResultsSinceCompactedForAgent(since, rawCutoff time.Time, agent string) ([]model.Result, error) {
	s.resultsSinceCalls++
	return nil, nil
}

func (s *fakeStore) StreamResultsSince(since time.Time, agent string, fn func(model.Result) error) error {
	s.resultsSinceCalls++
	for _, result := range s.streamedResults {
		if agent != "" && result.Agent != agent {
			continue
		}
		if result.CheckedAt.Before(since) {
			continue
		}
		if err := fn(result); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStore) StreamResultsSinceCompacted(since, rawCutoff time.Time, agent string, fn func(model.Result) error) error {
	return s.StreamResultsSince(since, agent, fn)
}

func (s *fakeStore) RollupBefore(cutoff time.Time, interval time.Duration) (int, error) {
	return 0, nil
}

func (s *fakeStore) DeleteBefore(cutoff time.Time) (int, error) {
	return 0, nil
}

func (s *fakeStore) DeleteRollupsBefore(cutoff time.Time) (int, error) {
	return 0, nil
}

func (s *fakeStore) Vacuum() error {
	return nil
}

func (s *fakeStore) ConsecutiveFailures(targetName, address string, port int, limit int) (int, error) {
	return 0, nil
}

func TestHandleDashboardResultsStreamsFromDB(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		streamedResults: []model.Result{
			{
				Agent:            "agent-1",
				AgentIP:          "203.0.113.1",
				TargetName:       "web",
				Address:          "198.51.100.10",
				Port:             443,
				Labels:           []string{"prod"},
				CheckedAt:        now.Add(-6 * 24 * time.Hour),
				SuccessCount:     1,
				AverageLatencyMS: 10,
				SuccessRate:      1,
			},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=7d&agent=agent-1", nil)
	rr := httptest.NewRecorder()

	s.handleResults(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if store.resultsSinceCalls != 1 {
		t.Fatalf("resultsSinceCalls = %d, want 1", store.resultsSinceCalls)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != 1 || got[0].Agent != "agent-1" {
		t.Fatalf("streamed response = %+v", got)
	}
}

func TestDashboardKeepsDetailChartVisibleWithoutRangeData(t *testing.T) {
	assets := dashboardHTML + dashboardJS
	for _, want := range []string{
		"当前周期暂无数据",
		"const span = Math.max(minChartGapMs, parseRangeMillis(selectedRange));",
		"this.emptyState.style.display = hasVisibleData ? 'none' : 'flex';",
	} {
		if !strings.Contains(assets, want) {
			t.Fatalf("dashboard assets missing empty detail chart behavior %q", want)
		}
	}
}

func TestHandleDashboardResultsLandingStreamsAllAgents(t *testing.T) {
	store := &fakeStore{
		streamedResults: []model.Result{
			{
				Agent:            "agent-1",
				AgentIP:          "203.0.113.1",
				TargetName:       "web",
				Address:          "198.51.100.10",
				Port:             443,
				CheckedAt:        time.Now().UTC(),
				SuccessCount:     1,
				AverageLatencyMS: 10,
				SuccessRate:      1,
			},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h", nil)
	rr := httptest.NewRecorder()

	s.handleResults(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if store.resultsSinceCalls != 1 {
		t.Fatalf("resultsSinceCalls = %d, want 1", store.resultsSinceCalls)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) != 1 || got[0].Agent != "agent-1" {
		t.Fatalf("landing streamed response = %+v", got)
	}
}

func TestDashboardStreamsEachRequestedRange(t *testing.T) {
	now := time.Now().UTC()
	results := []model.Result{
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now, SuccessCount: 1, AverageLatencyMS: 10, SuccessRate: 1},
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now.Add(-30 * time.Minute), SuccessCount: 1, FailureCount: 1, AverageLatencyMS: 11, SuccessRate: 0.5, Error: "timeout"},
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "db", Address: "198.51.100.20", Port: 5432, CheckedAt: now.Add(-2 * time.Hour), SuccessCount: 1, AverageLatencyMS: 5, SuccessRate: 1},
	}
	store := &fakeStore{streamedResults: results}
	s := &server{cfg: config.DefaultConfig(), store: store}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", rr.Code)
	}
	firstGot := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(firstGot) != 3 {
		t.Fatalf("first request returned %d rows, want 3", len(firstGot))
	}
	firstCalls := store.resultsSinceCalls

	req2 := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=1h&agent=agent-1", nil)
	rr2 := httptest.NewRecorder()
	s.handleResults(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("second request status = %d, want 200", rr2.Code)
	}
	secondGot := decodeDashboardResultsForTest(t, rr2.Body.Bytes())
	if len(secondGot) != 2 {
		t.Fatalf("second request (1h) returned %d rows, want 2", len(secondGot))
	}
	if store.resultsSinceCalls != firstCalls+1 {
		t.Fatalf("second request calls = %d, want %d", store.resultsSinceCalls, firstCalls+1)
	}
}

func TestDashboardStreamsLongRange(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		streamedResults: []model.Result{
			{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now, SuccessCount: 1, AverageLatencyMS: 10, SuccessRate: 1},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=365d&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if store.resultsSinceCalls != 1 {
		t.Fatalf("resultsSinceCalls = %d, want 1", store.resultsSinceCalls)
	}
}

func TestDashboardRefreshesAfterNewData(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		streamedResults: []model.Result{
			{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "198.51.100.10", Port: 443, CheckedAt: now, SuccessCount: 1, AverageLatencyMS: 10, SuccessRate: 1},
		},
	}
	s := &server{cfg: config.DefaultConfig(), store: store, hub: newWebsocketHub(), resultsCache: newResultsCache(time.Minute, 16)}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)
	callsAfterFirst := store.resultsSinceCalls
	cachedReq := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	cachedRR := httptest.NewRecorder()
	s.handleResults(cachedRR, cachedReq)
	if store.resultsSinceCalls != callsAfterFirst {
		t.Fatalf("dashboard cache miss: calls = %d, want %d", store.resultsSinceCalls, callsAfterFirst)
	}

	body := `{"agent":"agent-1","agent_ip":"203.0.113.1","target_name":"web","address":"198.51.100.10","port":443,"checked_at":"2026-06-20T10:00:00Z","success_count":1,"failure_count":0,"average_latency_ms":1,"success_rate":1}`
	reportReq := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader(body))
	reportRR := httptest.NewRecorder()
	s.handleReport(reportRR, reportReq)

	req2 := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	rr2 := httptest.NewRecorder()
	s.handleResults(rr2, req2)
	if store.resultsSinceCalls <= callsAfterFirst {
		t.Fatalf("cache should be invalidated after new data (calls: %d, was: %d)", store.resultsSinceCalls, callsAfterFirst)
	}
}

func decodeDashboardResultsForTest(t *testing.T, data []byte) []model.Result {
	t.Helper()
	var rows [][]json.RawMessage
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("decode dashboard cache response: %v", err)
	}
	results := make([]model.Result, 0, len(rows))
	for i, row := range rows {
		if len(row) != 12 {
			t.Fatalf("row %d has %d columns, want 12", i, len(row))
		}
		var result model.Result
		var checkedAt string
		unmarshalColumn := func(column int, dst any) {
			t.Helper()
			if err := json.Unmarshal(row[column], dst); err != nil {
				t.Fatalf("decode row %d column %d: %v", i, column, err)
			}
		}
		unmarshalColumn(0, &result.Agent)
		unmarshalColumn(1, &result.AgentIP)
		unmarshalColumn(2, &result.TargetName)
		unmarshalColumn(3, &result.Address)
		unmarshalColumn(4, &result.Port)
		unmarshalColumn(5, &result.Labels)
		unmarshalColumn(6, &checkedAt)
		parsedAt, err := parseDashboardTimeForTest(checkedAt)
		if err != nil {
			t.Fatalf("parse row %d checked_at: %v", i, err)
		}
		result.CheckedAt = parsedAt
		unmarshalColumn(7, &result.SuccessCount)
		unmarshalColumn(8, &result.FailureCount)
		unmarshalColumn(9, &result.AverageLatencyMS)
		unmarshalColumn(10, &result.SuccessRate)
		unmarshalColumn(11, &result.Error)
		results = append(results, result)
	}
	return results
}

func parseDashboardTimeForTest(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	nanos, err := strconv.ParseInt(value, 36, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanos).UTC(), nil
}

func assertResultEqual(t *testing.T, got, want model.Result) {
	t.Helper()
	if got.Agent != want.Agent ||
		got.AgentIP != want.AgentIP ||
		got.TargetName != want.TargetName ||
		got.Address != want.Address ||
		got.Port != want.Port ||
		!equalLabels(got.Labels, want.Labels) ||
		!got.CheckedAt.Equal(want.CheckedAt) ||
		got.SuccessCount != want.SuccessCount ||
		got.FailureCount != want.FailureCount ||
		got.AverageLatencyMS != want.AverageLatencyMS ||
		got.SuccessRate != want.SuccessRate ||
		got.Error != want.Error {
		t.Fatalf("result = %+v, want %+v", got, want)
	}
}

func equalLabels(got, want []string) bool {
	if len(got) == 0 && len(want) == 0 {
		return true
	}
	return reflect.DeepEqual(got, want)
}

func TestHandleTasksSavesAgentHeartbeat(t *testing.T) {
	store := &fakeStore{}
	s := &server{cfg: config.DefaultConfig(), store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/tasks?agent=agent-1&agent_ip=203.0.113.10", nil)
	rr := httptest.NewRecorder()

	s.handleTasks(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if len(store.heartbeats) != 1 {
		t.Fatalf("heartbeats = %d, want 1", len(store.heartbeats))
	}
	if store.heartbeats[0].agent != "agent-1" || store.heartbeats[0].agentIP != "203.0.113.10" {
		t.Fatalf("heartbeat = %+v", store.heartbeats[0])
	}
}

func TestHandleReportSavesAgentHeartbeat(t *testing.T) {
	store := &fakeStore{}
	s := &server{cfg: config.DefaultConfig(), store: store, hub: newWebsocketHub()}
	body := `{"agent":"agent-1","agent_ip":"203.0.113.10","target_name":"web","address":"198.51.100.10","port":443,"checked_at":"2026-06-20T10:00:00Z","success_count":3,"failure_count":0,"average_latency_ms":4.2,"success_rate":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader(body))
	rr := httptest.NewRecorder()

	s.handleReport(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if len(store.results) != 1 {
		t.Fatalf("results = %d, want 1", len(store.results))
	}
	if len(store.heartbeats) != 1 {
		t.Fatalf("heartbeats = %d, want 1", len(store.heartbeats))
	}
	if store.heartbeats[0].agent != "agent-1" || store.heartbeats[0].agentIP != "203.0.113.10" {
		t.Fatalf("heartbeat = %+v", store.heartbeats[0])
	}
}

func TestHandleReportRejectsEmptyObject(t *testing.T) {
	store := &fakeStore{}
	status, body := postReport(t, store, `{}`)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsNull(t *testing.T) {
	store := &fakeStore{}
	status, body := postReport(t, store, `null`)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsMissingRequiredFieldsInArray(t *testing.T) {
	store := &fakeStore{}
	status, body := postReport(t, store, `[{}]`)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsInvalidPort(t *testing.T) {
	for _, port := range []int{0, 70000} {
		t.Run("port", func(t *testing.T) {
			store := &fakeStore{}
			result := validReportResult()
			result.Port = port

			status, body := postReport(t, store, reportJSON(t, result))

			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", status, body)
			}
			assertNoReportWrites(t, store)
		})
	}
}

func TestHandleReportRejectsNegativeCounts(t *testing.T) {
	tests := []struct {
		name string
		edit func(*model.Result)
	}{
		{name: "success_count", edit: func(result *model.Result) { result.SuccessCount = -1 }},
		{name: "failure_count", edit: func(result *model.Result) { result.FailureCount = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{}
			result := validReportResult()
			tt.edit(&result)

			status, body := postReport(t, store, reportJSON(t, result))

			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", status, body)
			}
			assertNoReportWrites(t, store)
		})
	}
}

func TestHandleReportRejectsZeroSamples(t *testing.T) {
	store := &fakeStore{}
	result := validReportResult()
	result.SuccessCount = 0
	result.FailureCount = 0

	status, body := postReport(t, store, reportJSON(t, result))

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	assertNoReportWrites(t, store)
}

func TestHandleReportRejectsInvalidSuccessRate(t *testing.T) {
	for _, successRate := range []float64{-0.1, 1.1} {
		t.Run("success_rate", func(t *testing.T) {
			store := &fakeStore{}
			result := validReportResult()
			result.SuccessRate = successRate

			status, body := postReport(t, store, reportJSON(t, result))

			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", status, body)
			}
			assertNoReportWrites(t, store)
		})
	}
}

func TestHandleReportRejectsBatchWithOneInvalidResultWithoutPartialWrite(t *testing.T) {
	store := &fakeStore{}
	valid := validReportResult()
	body := `[` + reportJSON(t, valid) + `,{}]`

	status, response := postReport(t, store, body)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, response)
	}
	assertNoReportWrites(t, store)
}

func TestValidateReportResult(t *testing.T) {
	tests := []struct {
		name string
		edit func(*model.Result)
	}{
		{name: "valid", edit: func(*model.Result) {}},
		{name: "agent required", edit: func(result *model.Result) { result.Agent = " \t" }},
		{name: "target_name required", edit: func(result *model.Result) { result.TargetName = "" }},
		{name: "address required", edit: func(result *model.Result) { result.Address = "" }},
		{name: "invalid port low", edit: func(result *model.Result) { result.Port = 0 }},
		{name: "invalid port high", edit: func(result *model.Result) { result.Port = 65536 }},
		{name: "negative success count", edit: func(result *model.Result) { result.SuccessCount = -1 }},
		{name: "negative failure count", edit: func(result *model.Result) { result.FailureCount = -1 }},
		{name: "zero samples", edit: func(result *model.Result) { result.SuccessCount = 0 }},
		{name: "negative latency", edit: func(result *model.Result) { result.AverageLatencyMS = -0.01 }},
		{name: "success rate low", edit: func(result *model.Result) { result.SuccessRate = -0.01 }},
		{name: "success rate high", edit: func(result *model.Result) { result.SuccessRate = 1.01 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validReportResult()
			tt.edit(&result)

			err := validateReportResult(result)

			if tt.name == "valid" && err != nil {
				t.Fatalf("validateReportResult() error = %v, want nil", err)
			}
			if tt.name != "valid" && err == nil {
				t.Fatalf("validateReportResult() error = nil, want error")
			}
		})
	}
}

func validReportResult() model.Result {
	return model.Result{
		Agent:            "agent-1",
		AgentIP:          "203.0.113.10",
		TargetName:       "web",
		Address:          "198.51.100.10",
		Port:             443,
		CheckedAt:        time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC),
		SuccessCount:     3,
		FailureCount:     0,
		AverageLatencyMS: 4.2,
		SuccessRate:      1,
	}
}

func reportJSON(t *testing.T, result model.Result) string {
	t.Helper()
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	return string(body)
}

func postReport(t *testing.T, store *fakeStore, body string) (int, string) {
	t.Helper()
	s := &server{cfg: config.DefaultConfig(), store: store, hub: newWebsocketHub()}
	req := httptest.NewRequest(http.MethodPost, "/api/report", strings.NewReader(body))
	rr := httptest.NewRecorder()

	s.handleReport(rr, req)

	return rr.Code, rr.Body.String()
}

func assertNoReportWrites(t *testing.T, store *fakeStore) {
	t.Helper()
	if len(store.results) != 0 {
		t.Fatalf("results = %d, want 0", len(store.results))
	}
	if len(store.heartbeats) != 0 {
		t.Fatalf("heartbeats = %d, want 0", len(store.heartbeats))
	}
}

func TestAggregateRowsByTime_NoAggregationNeeded(t *testing.T) {
	now := time.Now()
	rows := []agentRow{
		{checkedAt: now.UnixNano(), data: []byte("[1]")},
		{checkedAt: now.Add(-1 * time.Hour).UnixNano(), data: []byte("[2]")},
	}
	got := aggregateRowsByTime(rows, now.Add(-2*time.Hour), 3000)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
}

func TestAggregateRowsByTime_EmptyInput(t *testing.T) {
	rows := aggregateRowsByTime(nil, time.Now(), 3000)
	if len(rows) != 0 {
		t.Fatalf("got %d rows, want 0", len(rows))
	}
}

func TestAggregateRowsByTime_SameBucket(t *testing.T) {
	base := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	rows := make([]agentRow, 100)
	for i := 0; i < 100; i++ {
		rows[i] = agentRow{
			checkedAt: base.Add(time.Duration(99-i) * time.Second).UnixNano(),
			data:      []byte("[x]"),
		}
	}
	since := base
	targetCount := 10
	bucketNanos := int64(base.Add(99 * time.Second).Sub(since))
	_ = bucketNanos
	got := aggregateRowsByTime(rows, since, targetCount)
	if len(got) > targetCount {
		t.Fatalf("got %d rows, want at most %d", len(got), targetCount)
	}
	if len(got) == 0 {
		t.Fatal("expected at least 1 row")
	}
	for i := 1; i < len(got); i++ {
		if got[i].checkedAt >= got[i-1].checkedAt {
			t.Errorf("rows not strictly decreasing: got[%d]=%d, got[%d]=%d", i-1, got[i-1].checkedAt, i, got[i].checkedAt)
		}
	}
}

func TestAggregateRowsByTime_ProducesDistinctRows(t *testing.T) {
	base := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	rows := make([]agentRow, 500)
	for i := 0; i < 500; i++ {
		rows[i] = agentRow{
			checkedAt: base.Add(time.Duration(499-i) * time.Minute).UnixNano(),
			data:      []byte("[x]"),
		}
	}
	since := base
	got := aggregateRowsByTime(rows, since, 50)
	seen := make(map[int64]bool)
	for _, r := range got {
		if seen[r.checkedAt] {
			t.Errorf("duplicate checkedAt %d in aggregated output", r.checkedAt)
		}
		seen[r.checkedAt] = true
	}
	if len(got) > 50 {
		t.Fatalf("got %d rows, want at most 50", len(got))
	}
}

func TestAggregateRowsByTime_ExactTargetCount(t *testing.T) {
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	n := int64(10000)
	rows := make([]agentRow, n)
	for i := int64(0); i < n; i++ {
		rows[i] = agentRow{
			checkedAt: base.Add(time.Duration(n-1-i) * time.Second).UnixNano(),
			data:      []byte("[x]"),
		}
	}
	got := aggregateRowsByTime(rows, base, 3000)
	if len(got) != 3000 {
		t.Fatalf("got %d rows, want 3000", len(got))
	}
}

func TestAggregateRowsByTime_PreservesEveryTargetSeries(t *testing.T) {
	base := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	const targetTotal = 12
	const samplesPerTarget = 400
	rows := make([]agentRow, 0, targetTotal*samplesPerTarget)
	for sample := 0; sample < samplesPerTarget; sample++ {
		checkedAt := base.Add(-time.Duration(sample) * time.Minute)
		for target := 0; target < targetTotal; target++ {
			result := model.Result{
				Agent:            "agent-1",
				TargetName:       fmt.Sprintf("target-%02d", target),
				Address:          fmt.Sprintf("192.0.2.%d", target+1),
				Port:             443,
				CheckedAt:        checkedAt,
				SuccessCount:     1,
				AverageLatencyMS: float64(target + sample%10),
				SuccessRate:      1,
			}
			row, err := newAgentRow(result)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	}

	got := aggregateRowsByTime(rows, base.Add(-samplesPerTarget*time.Minute), maxChartPoints)
	if len(got) > maxChartPoints {
		t.Fatalf("got %d rows, want at most %d", len(got), maxChartPoints)
	}
	seriesCounts := make(map[string]int)
	for _, row := range got {
		var parts []interface{}
		if err := json.Unmarshal(row.data, &parts); err != nil {
			t.Fatal(err)
		}
		seriesCounts[parts[2].(string)]++
	}
	if len(seriesCounts) != targetTotal {
		t.Fatalf("got %d target series, want %d", len(seriesCounts), targetTotal)
	}
	for target, count := range seriesCounts {
		if count < 2 {
			t.Errorf("series %q has %d point(s), want at least 2", target, count)
		}
	}
}

func TestResultsCacheEvictsWhenCapacityExceeded(t *testing.T) {
	cache := newResultsCache(time.Minute, 2)
	cache.set(resultsCacheKey{selectedRange: "1h"}, []byte("one"))
	cache.set(resultsCacheKey{selectedRange: "2h"}, []byte("two"))
	cache.set(resultsCacheKey{selectedRange: "3h"}, []byte("three"))
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if len(cache.entries) != 2 {
		t.Fatalf("cache entries = %d, want 2", len(cache.entries))
	}
}

func TestAggregateRowsByTime_SpanTooSmall(t *testing.T) {
	now := time.Now()
	rows := []agentRow{
		{checkedAt: now.UnixNano(), data: []byte("[1]")},
		{checkedAt: now.UnixNano(), data: []byte("[2]")},
	}
	got := aggregateRowsByTime(rows, now, 5)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2 (span too small, return all)", len(got))
	}
}

func TestAggregateRowsByTime_BucketNanosTooSmall(t *testing.T) {
	base := time.Unix(0, 0)
	rows := []agentRow{
		{checkedAt: base.Add(10 * time.Nanosecond).UnixNano(), data: []byte("[1]")},
		{checkedAt: base.Add(9 * time.Nanosecond).UnixNano(), data: []byte("[2]")},
		{checkedAt: base.Add(8 * time.Nanosecond).UnixNano(), data: []byte("[3]")},
	}
	got := aggregateRowsByTime(rows, base, 100)
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3 (bucket too small, return all)", len(got))
	}
}

func TestDashboardStreamsRequestedRange(t *testing.T) {
	now := time.Now().UTC()
	results := []model.Result{
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "192.0.2.1", Port: 443, CheckedAt: now, SuccessCount: 1, AverageLatencyMS: 10, SuccessRate: 1},
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "192.0.2.1", Port: 443, CheckedAt: now.Add(-30 * time.Minute), SuccessCount: 1, FailureCount: 1, AverageLatencyMS: 11, SuccessRate: 0.5, Error: "timeout"},
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "db", Address: "192.0.2.2", Port: 5432, CheckedAt: now.Add(-2 * time.Hour), SuccessCount: 1, AverageLatencyMS: 5, SuccessRate: 1},
		{Agent: "agent-1", AgentIP: "203.0.113.1", TargetName: "web", Address: "192.0.2.1", Port: 443, CheckedAt: now.Add(-4 * time.Hour), SuccessCount: 0, FailureCount: 3, AverageLatencyMS: 0, SuccessRate: 0, Error: "refused"},
	}
	store := &fakeStore{streamedResults: results}
	s := &server{cfg: config.DefaultConfig(), store: store}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=24h&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", rr.Code)
	}
	firstGot := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(firstGot) != 4 {
		t.Fatalf("first request returned %d rows, want 4", len(firstGot))
	}

	req2 := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=1h&agent=agent-1", nil)
	rr2 := httptest.NewRecorder()
	s.handleResults(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("second request status = %d, want 200", rr2.Code)
	}
	secondGot := decodeDashboardResultsForTest(t, rr2.Body.Bytes())
	if len(secondGot) != 2 {
		t.Fatalf("second request (1h) returned %d rows, want 2", len(secondGot))
	}
}

func TestAggregateRowsByTime_DirectStreamAggregatesToChartPoints(t *testing.T) {
	base := time.Now().UTC().Add(-7 * 24 * time.Hour)
	results := make([]model.Result, 5000)
	for i := 0; i < 5000; i++ {
		results[i] = model.Result{
			Agent:            "agent-1",
			AgentIP:          "203.0.113.1",
			TargetName:       "web",
			Address:          "192.0.2.1",
			Port:             443,
			CheckedAt:        base.Add(time.Duration(4999-i) * (10 * time.Minute)).UTC(),
			SuccessCount:     1,
			AverageLatencyMS: float64(10 + (i % 5)),
			SuccessRate:      1,
		}
	}
	store := &fakeStore{streamedResults: results}
	s := &server{cfg: config.DefaultConfig(), store: store}

	req := httptest.NewRequest(http.MethodGet, "/api/results?dashboard=1&range=365d&agent=agent-1", nil)
	rr := httptest.NewRecorder()
	s.handleResults(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	got := decodeDashboardResultsForTest(t, rr.Body.Bytes())
	if len(got) == 0 {
		t.Fatal("direct stream returned 0 rows")
	}
	if len(got) > maxChartPoints {
		t.Fatalf("direct stream returned %d rows, want at most %d", len(got), maxChartPoints)
	}
	seen := make(map[int64]bool)
	for _, r := range got {
		key := r.CheckedAt.UnixNano()
		if seen[key] {
			t.Errorf("duplicate timestamp %d in direct stream output", key)
		}
		seen[key] = true
	}
}
